package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"time"

	// "mime/multipart"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				createdBy = s.Email
				break
			}
		}
		if createdBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := context.Background()
		created := make([]map[string]interface{}, 0)

		for _, rrow := range req.Rows {
			var missing []string
			if strings.TrimSpace(rrow.TypeName) == "" {
				missing = append(missing, "type_name")
			}
			if strings.TrimSpace(rrow.Status) == "" {
				missing = append(missing, "status")
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
				created = append(created, map[string]interface{}{"success": false, "error": fmt.Sprintf("missing required fields: %s", strings.Join(missing, ", "))})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				created = append(created, map[string]interface{}{"success": false, "error": "failed to start tx: " + err.Error()})
				continue
			}

			var id string
			sp := "sp_" + strings.ReplaceAll(uuid.New().String(), "-", "_")
			if _, err := tx.Exec(ctx, "SAVEPOINT "+sp); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": "failed to create savepoint: " + err.Error()})
				continue
			}

			if err := tx.QueryRow(ctx, `SELECT 'TYP-' || LPAD(nextval('payable_receivable_seq')::text, 6, '0')`).Scan(&id); err != nil {
				tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+sp)
				created = append(created, map[string]interface{}{"success": false, "error": "failed to generate type_id: " + err.Error(), "type_name": rrow.TypeName})
				tx.Commit(ctx)
				continue
			}
			var effFrom, effTo interface{}
			if strings.TrimSpace(rrow.EffectiveFrom) != "" {
				if norm := NormalizeDate(rrow.EffectiveFrom); norm != "" {
					if tval, err := time.Parse("2006-01-02", norm); err == nil {
						effFrom = tval
					}
				}
			}
			if strings.TrimSpace(rrow.EffectiveTo) != "" {
				if norm := NormalizeDate(rrow.EffectiveTo); norm != "" {
					if tval, err := time.Parse("2006-01-02", norm); err == nil {
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
				created = append(created, map[string]interface{}{"success": false, "error": err.Error(), "type_name": rrow.TypeName})
				tx.Commit(ctx)
				continue
			}

			auditQ := `INSERT INTO auditactionpayablereceivable (type_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, auditQ, id, nil, createdBy); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": "audit insert failed: " + err.Error(), "id": id})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": "commit failed: " + err.Error(), "id": id})
				continue
			}
			created = append(created, map[string]interface{}{"success": true, "type_id": id, "type_name": rrow.TypeName})
		}

		overall := true
		for _, r := range created {
			if ok, found := r["success"].(bool); !found || !ok {
				overall = false
				break
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": overall, "rows": created})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := r.Context()

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
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
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

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
	}
}

func GetApprovedActivePayableReceivable(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		valid := false
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := r.Context()
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
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
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

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		// get updated_by
		updatedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				updatedBy = s.Email
				break
			}
		}
		if updatedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := r.Context()
		results := []map[string]interface{}{}
		for _, row := range req.Rows {
			if row.TypeID == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "missing type_id"})
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "begin failed: " + err.Error()})
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
					results = append(results, map[string]interface{}{"success": false, "error": "fetch failed: " + err.Error(), "type_id": row.TypeID})
					return
				}

				var sets []string
				var args []interface{}
				pos := 1
				for k, v := range row.Fields {
					switch k {
					case "type_code":
						sets = append(sets, fmt.Sprintf("type_code=$%d, old_type_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTypeCode))
						pos += 2
					case "type_name":
						sets = append(sets, fmt.Sprintf("type_name=$%d, old_type_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTypeName))
						pos += 2
					case "direction":
						sets = append(sets, fmt.Sprintf("direction=$%d, old_direction=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingDirection))
						pos += 2
					case "business_unit_division":
						sets = append(sets, fmt.Sprintf("business_unit_division=$%d, old_business_unit_division=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingBusinessUnitDivision))
						pos += 2
					case "default_currency":
						sets = append(sets, fmt.Sprintf("default_currency=$%d, old_default_currency=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingDefaultCurrency))
						pos += 2
					case "default_due_days":
						sets = append(sets, fmt.Sprintf("default_due_days=$%d, old_default_due_days=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingDefaultDueDays))
						pos += 2
					case "payment_terms_name":
						sets = append(sets, fmt.Sprintf("payment_terms_name=$%d, old_payment_terms_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingPaymentTermsName))
						pos += 2
					case "allow_netting":
						sets = append(sets, fmt.Sprintf("allow_netting=$%d, old_allow_netting=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingAllowNetting))
						pos += 2
					case "settlement_discount":
						sets = append(sets, fmt.Sprintf("settlement_discount=$%d, old_settlement_discount=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSettlementDiscount))
						pos += 2
					case "settlement_discount_percent":
						sets = append(sets, fmt.Sprintf("settlement_discount_percent=$%d, old_settlement_discount_percent=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSettlementDiscountPercent))
						pos += 2
					case "tax_applicable":
						sets = append(sets, fmt.Sprintf("tax_applicable=$%d, old_tax_applicable=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTaxApplicable))
						pos += 2
					case "tax_code":
						sets = append(sets, fmt.Sprintf("tax_code=$%d, old_tax_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTaxCode))
						pos += 2
					case "default_recon_gl":
						sets = append(sets, fmt.Sprintf("default_recon_gl=$%d, old_default_recon_gl=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingDefaultReconGL))
						pos += 2
					case "offset_revenue_expense_gl":
						sets = append(sets, fmt.Sprintf("offset_revenue_expense_gl=$%d, old_offset_revenue_expense_gl=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOffsetRevenueExpenseGL))
						pos += 2
					case "cash_flow_category":
						sets = append(sets, fmt.Sprintf("cash_flow_category=$%d, old_cash_flow_category=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCashFlowCategory))
						pos += 2
					case "category":
						sets = append(sets, fmt.Sprintf("category=$%d, old_category=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCategory))
						pos += 2
					case "effective_from":
						sets = append(sets, fmt.Sprintf("effective_from=$%d, old_effective_from=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingEffectiveFrom))
						pos += 2
					case "effective_to":
						sets = append(sets, fmt.Sprintf("effective_to=$%d, old_effective_to=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingEffectiveTo))
						pos += 2
					case "tags":
						sets = append(sets, fmt.Sprintf("tags=$%d, old_tags=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTags))
						pos += 2
					case "erp_type":
						sets = append(sets, fmt.Sprintf("erp_type=$%d, old_erp_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingErpType))
						pos += 2
					case "sap_company_code":
						sets = append(sets, fmt.Sprintf("sap_company_code=$%d, old_sap_company_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapCompanyCode))
						pos += 2
					case "sap_fi_doc_type":
						sets = append(sets, fmt.Sprintf("sap_fi_doc_type=$%d, old_sap_fi_doc_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapFiDocType))
						pos += 2
					case "sap_posting_key_debit":
						sets = append(sets, fmt.Sprintf("sap_posting_key_debit=$%d, old_sap_posting_key_debit=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapPostingKeyDebit))
						pos += 2
					case "sap_posting_key_credit":
						sets = append(sets, fmt.Sprintf("sap_posting_key_credit=$%d, old_sap_posting_key_credit=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapPostingKeyCredit))
						pos += 2
					case "sap_reconciliation_gl":
						sets = append(sets, fmt.Sprintf("sap_reconciliation_gl=$%d, old_sap_reconciliation_gl=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapReconciliationGL))
						pos += 2
					case "sap_tax_code":
						sets = append(sets, fmt.Sprintf("sap_tax_code=$%d, old_sap_tax_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapTaxCode))
						pos += 2
					case "oracle_ledger":
						sets = append(sets, fmt.Sprintf("oracle_ledger=$%d, old_oracle_ledger=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleLedger))
						pos += 2
					case "oracle_transaction_type":
						sets = append(sets, fmt.Sprintf("oracle_transaction_type=$%d, old_oracle_transaction_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleTransactionType))
						pos += 2
					case "oracle_distribution_set":
						sets = append(sets, fmt.Sprintf("oracle_distribution_set=$%d, old_oracle_distribution_set=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleDistributionSet))
						pos += 2
					case "oracle_source":
						sets = append(sets, fmt.Sprintf("oracle_source=$%d, old_oracle_source=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleSource))
						pos += 2
					case "tally_voucher_type":
						sets = append(sets, fmt.Sprintf("tally_voucher_type=$%d, old_tally_voucher_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTallyVoucherType))
						pos += 2
					case "tally_tax_class":
						sets = append(sets, fmt.Sprintf("tally_tax_class=$%d, old_tally_tax_class=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTallyTaxClass))
						pos += 2
					case "tally_ledger_group":
						sets = append(sets, fmt.Sprintf("tally_ledger_group=$%d, old_tally_ledger_group=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTallyLedgerGroup))
						pos += 2
					case "sage_nominal_control":
						sets = append(sets, fmt.Sprintf("sage_nominal_control=$%d, old_sage_nominal_control=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSageNominalControl))
						pos += 2
					case "sage_analysis_code":
						sets = append(sets, fmt.Sprintf("sage_analysis_code=$%d, old_sage_analysis_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSageAnalysisCode))
						pos += 2
					case "external_code":
						sets = append(sets, fmt.Sprintf("external_code=$%d, old_external_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingExternalCode))
						pos += 2
					case "segment":
						sets = append(sets, fmt.Sprintf("segment=$%d, old_segment=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSegment))
						pos += 2
					case "status":
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingStatus))
						pos += 2
					default:
					}
				}

				updatedID := row.TypeID
				if len(sets) > 0 {
					q := "UPDATE masterpayablereceivabletype SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE type_id=$%d RETURNING type_id", pos)
					args = append(args, row.TypeID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedID); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "update failed: " + err.Error(), "type_id": row.TypeID})
						return
					}
				}

				// audit
				auditQ := `INSERT INTO auditactionpayablereceivable (type_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL', $2, $3, now())`
				if _, err := tx.Exec(ctx, auditQ, updatedID, row.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "audit failed: " + err.Error(), "type_id": updatedID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "commit failed: " + err.Error(), "type_id": updatedID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{"success": true, "type_id": updatedID})
			}()
		}

		overall := true
		for _, r := range results {
			if ok, exists := r["success"]; exists {
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": overall, "rows": results})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		// validate session / user
		requestedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				requestedBy = s.Email
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		if len(req.TypeIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "type_ids required")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to start transaction: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		committed = true

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		checkerBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to start transaction: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		committed = true

		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		checkerBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to start transaction: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		committed = true

		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}
func UploadPayableReceivable(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := ""
		if r.Header.Get("Content-Type") == "application/json" {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in body")
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue("user_id")
			if userID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in form")
				return
			}
		}
		userName := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "User not found in active sessions")
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse multipart form")
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No files uploaded")
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

			columns := append([]string{"upload_batch_id"}, headerNorm...)

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction: "+err.Error())
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()

			// stage
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"input_payablereceivable_table"}, columns, pgx.CopyFromRows(copyRows))
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}

			// read mapping
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_payablereceivable`)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Mapping error")
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
			var srcCols []string
			var tgtCols []string
			for i, h := range headerRow {
				key := headerNorm[i]
				if t, ok := mapping[key]; ok {
					srcCols = append(srcCols, key)
					tgtCols = append(tgtCols, t)
				} else {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("No mapping for source column: %s", h))
					return
				}
			}
			tgtColsStr := strings.Join(tgtCols, ", ")

			var selectExprs []string
			for i, src := range srcCols {
				tgt := tgtCols[i]
				selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", src, tgt))
			}
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

			if len(newIDs) > 0 {
				auditSQL := `INSERT INTO auditactionpayablereceivable (type_id, actiontype, processing_status, reason, requested_by, requested_at) SELECT type_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now() FROM masterpayablereceivabletype WHERE type_id = ANY($2)`
				if _, err := tx.Exec(ctx, auditSQL, userName, newIDs); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit actions: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			committed = true
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "batch_ids": batchIDs})
	}
}
