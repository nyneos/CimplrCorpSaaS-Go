package approvalengine

import (
	"CimplrCorpSaas/api"
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const latestAuditOrderSQL = `GREATEST(COALESCE(checker_at, requested_at), requested_at) DESC NULLS LAST`

type instanceHealSpec struct {
	ModuleCode    string
	IDPrefix      string
	RecordTable   string
	AuditTable    string
	AuditIDColumn string
	TxPrefix      string
	ActionColumn  string // actiontype | action_type
	EntityColumn  string // optional column on the record table
	AmountColumn  string // optional column on the record table
}

var instanceHealSpecs = []instanceHealSpec{
	{ModuleCode: "CASH", IDPrefix: "BBAL-", RecordTable: "cimplrcorpsaas.bank_balances_manual", AuditTable: "public.auditactionbankbalances", AuditIDColumn: "balance_id", TxPrefix: "BANK_BALANCE", ActionColumn: "actiontype", AmountColumn: "balance_amount"},
	{ModuleCode: "CASH", IDPrefix: "TR-PAY-", RecordTable: "cimplrcorpsaas.tr_payables", AuditTable: "public.auditactionpayable", AuditIDColumn: "payable_id", TxPrefix: "PAYABLE", ActionColumn: "actiontype", EntityColumn: "entity_name", AmountColumn: "amount"},
	{ModuleCode: "CASH", IDPrefix: "TR-REC-", RecordTable: "cimplrcorpsaas.tr_receivables", AuditTable: "public.auditactionreceivable", AuditIDColumn: "receivable_id", TxPrefix: "RECEIVABLE", ActionColumn: "actiontype", EntityColumn: "entity_name", AmountColumn: "invoice_amount"},
	{ModuleCode: "CASH", RecordTable: "cimplrcorpsaas.bank_limit", AuditTable: "cimplrcorpsaas.auditactionbanklimit", AuditIDColumn: "limit_id", TxPrefix: "BANK_LIMIT", ActionColumn: "action_type", EntityColumn: "entity_name", AmountColumn: "sanctioned_amount"},
	{ModuleCode: "CASH", RecordTable: "cimplrcorpsaas.bank_limit_utilization", AuditTable: "cimplrcorpsaas.auditactionbanklimitutilization", AuditIDColumn: "utilization_id", TxPrefix: "LIMIT_UTILIZATION", ActionColumn: "action_type", AmountColumn: "utilized_amount"},
	{ModuleCode: "CASH", RecordTable: "cimplrcorpsaas.sweepconfiguration", AuditTable: "cimplrcorpsaas.auditactionsweepconfiguration", AuditIDColumn: "sweep_id", TxPrefix: "SWEEP_CONFIG", ActionColumn: "actiontype", EntityColumn: "entity_name"},
	{ModuleCode: "CASH", RecordTable: "cimplrcorpsaas.sweep_initiation", AuditTable: "cimplrcorpsaas.auditactionsweepinitiation", AuditIDColumn: "initiation_id", TxPrefix: "SWEEP_INITIATION", ActionColumn: "actiontype", EntityColumn: "entity_name"},
	{ModuleCode: "CASH", RecordTable: "fund_plan_groups", AuditTable: "public.auditaction_fund_plan_groups", AuditIDColumn: "group_id", TxPrefix: "FUND_PLANNING", ActionColumn: "actiontype", EntityColumn: "entity_name", AmountColumn: "total_amount"},
	{ModuleCode: "CASH", RecordTable: "bank_statement", AuditTable: "public.auditactionbankstatement", AuditIDColumn: "bankstatementid", TxPrefix: "BANK_STATEMENT", ActionColumn: "actiontype"},
	{ModuleCode: "CASH", RecordTable: "cimplrcorpsaas.cashflow_proposal", AuditTable: "cimplrcorpsaas.audit_action_cashflow_proposal", AuditIDColumn: "proposal_id", TxPrefix: "CASH_FLOW_PROJECTION", ActionColumn: "action_type"},
	{ModuleCode: "INVESTMENT_MF", RecordTable: "investment.investment_proposal", AuditTable: "investment.auditactionproposal", AuditIDColumn: "proposal_id", TxPrefix: "MF_PROPOSAL", ActionColumn: "actiontype", EntityColumn: "entity_name", AmountColumn: "total_amount"},
	{ModuleCode: "INVESTMENT_MF", RecordTable: "investment.investment_initiation", AuditTable: "investment.auditactioninitiation", AuditIDColumn: "initiation_id", TxPrefix: "MF_INITIATION", ActionColumn: "actiontype", EntityColumn: "entity_name", AmountColumn: "amount"},
	{ModuleCode: "INVESTMENT_MF", RecordTable: "investment.investment_confirmation", AuditTable: "investment.auditactioninvestmentconfirmation", AuditIDColumn: "confirmation_id", TxPrefix: "MF_CONFIRMATION", ActionColumn: "actiontype", AmountColumn: "net_amount"},
	{ModuleCode: "INVESTMENT_MF", RecordTable: "investment.redemption_initiation", AuditTable: "investment.auditactionredemption", AuditIDColumn: "redemption_id", TxPrefix: "MF_REDEMPTION_INITIATION", ActionColumn: "actiontype", EntityColumn: "entity_name"},
	{ModuleCode: "INVESTMENT_MF", RecordTable: "investment.redemption_confirmation", AuditTable: "investment.auditactionredemptionconfirmation", AuditIDColumn: "redemption_confirm_id", TxPrefix: "MF_REDEMPTION_CONFIRMATION", ActionColumn: "actiontype"},
	{ModuleCode: "FIXED_DEPOSIT", RecordTable: "investment.fd_interest_receipt", AuditTable: "investment.fd_interest_receipt_audit", AuditIDColumn: "receipt_id", TxPrefix: "FD_RECEIPT", ActionColumn: "action_type", EntityColumn: "entity_id", AmountColumn: "gross_interest_received"},
	{ModuleCode: "FIXED_DEPOSIT", RecordTable: "investment.fd_rate_negotiation", AuditTable: "investment.fd_audit_rate_negotiation", AuditIDColumn: "rate_request_id", TxPrefix: "FD_RATE_NEGOTIATION", ActionColumn: "action_type", AmountColumn: "proposed_fd_amount"},
}

type healContext struct {
	Spec        instanceHealSpec
	ActionType  string
	Status      string
	EntityCode  string
	Amount      float64
	RequestedBy string
	Found       bool
}

func matchHealSpec(moduleCode, recordID string) *instanceHealSpec {
	id := strings.ToUpper(strings.TrimSpace(recordID))
	mod := strings.TrimSpace(moduleCode)
	for i := range instanceHealSpecs {
		spec := &instanceHealSpecs[i]
		if mod != "" && !strings.EqualFold(spec.ModuleCode, mod) {
			continue
		}
		if spec.IDPrefix != "" && strings.HasPrefix(id, spec.IDPrefix) {
			return spec
		}
	}
	return nil
}

func probeHealSpec(ctx context.Context, pool *pgxpool.Pool, moduleCode, recordID string) *instanceHealSpec {
	mod := strings.TrimSpace(moduleCode)
	for i := range instanceHealSpecs {
		spec := &instanceHealSpecs[i]
		if mod != "" && !strings.EqualFold(spec.ModuleCode, mod) {
			continue
		}
		if spec.IDPrefix != "" {
			continue
		}
		q := fmt.Sprintf(`SELECT 1 FROM %s WHERE %s::text = $1 LIMIT 1`, spec.AuditTable, spec.AuditIDColumn)
		var one int
		if err := pool.QueryRow(ctx, q, recordID).Scan(&one); err == nil {
			return spec
		}
	}
	return nil
}

func loadHealContext(ctx context.Context, pool *pgxpool.Pool, moduleCode, recordID string) healContext {
	var out healContext
	spec := matchHealSpec(moduleCode, recordID)
	if spec == nil {
		spec = probeHealSpec(ctx, pool, moduleCode, recordID)
	}
	if spec == nil {
		return out
	}
	out.Spec = *spec
	actionCol := spec.ActionColumn
	if actionCol == "" {
		actionCol = "action_type"
	}
	auditQ := fmt.Sprintf(`
		SELECT COALESCE(%s,''), COALESCE(processing_status,''), COALESCE(requested_by,'')
		FROM %s
		WHERE %s::text = $1
		ORDER BY %s
		LIMIT 1`, actionCol, spec.AuditTable, spec.AuditIDColumn, latestAuditOrderSQL)
	if err := pool.QueryRow(ctx, auditQ, recordID).Scan(&out.ActionType, &out.Status, &out.RequestedBy); err != nil {
		return out
	}
	out.Found = true
	if spec.RecordTable != "" && (spec.EntityColumn != "" || spec.AmountColumn != "") {
		entityExpr := `''`
		if spec.EntityColumn != "" {
			entityExpr = fmt.Sprintf(`COALESCE(%s::text,'')`, spec.EntityColumn)
		}
		amountExpr := `0::float8`
		if spec.AmountColumn != "" {
			amountExpr = fmt.Sprintf(`COALESCE(%s,0)`, spec.AmountColumn)
		}
		recQ := fmt.Sprintf(`SELECT %s, %s FROM %s WHERE %s::text = $1 LIMIT 1`,
			entityExpr, amountExpr, spec.RecordTable, spec.AuditIDColumn)
		_ = pool.QueryRow(ctx, recQ, recordID).Scan(&out.EntityCode, &out.Amount)
	}
	if spec.TxPrefix == "BANK_BALANCE" && out.EntityCode == "" {
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(mba.entity_id,'')
			FROM public.bank_balances_manual b
			LEFT JOIN (
				SELECT account_number, MIN(entity_id) AS entity_id
				FROM public.masterbankaccount
				GROUP BY account_number
			) mba ON mba.account_number = b.account_no
			WHERE b.balance_id = $1`, recordID).Scan(&out.EntityCode)
	}
	return out
}

func txTypeFromHeal(spec instanceHealSpec, actionType string) string {
	action := strings.ToUpper(strings.TrimSpace(actionType))
	switch action {
	case "EDIT", "DELETE", "CREATE":
		return spec.TxPrefix + "_" + action
	default:
		return spec.TxPrefix + "_CREATE"
	}
}

func isPendingAuditStatus(status string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(status)), "PENDING")
}

// EnsureInstanceForRecord creates a pending instance for a record whose audit
// is still PENDING but whose CreateInstance raced the HTTP response.
func EnsureInstanceForRecord(ctx context.Context, pool *pgxpool.Pool, moduleCode, recordID string) string {
	moduleCode = strings.TrimSpace(moduleCode)
	recordID = strings.TrimSpace(recordID)
	if recordID == "" {
		return ""
	}
	if existing, err := LookupLatestInstanceID(ctx, pool, moduleCode, recordID); err == nil && existing != "" {
		var st string
		_ = pool.QueryRow(ctx, `SELECT status FROM uam.approval_instance WHERE instance_id=$1 AND is_deleted=false`, existing).Scan(&st)
		if st == InstStatusPending {
			return existing
		}
	}
	h := loadHealContext(ctx, pool, moduleCode, recordID)
	if !h.Found || !isPendingAuditStatus(h.Status) {
		return ""
	}
	mod := h.Spec.ModuleCode
	if moduleCode != "" {
		mod = moduleCode
	}
	instID, err := CreateInstance(ctx, pool, InstanceRequest{
		ModuleCode:       mod,
		EntityCode:       h.EntityCode,
		TransactionType:  txTypeFromHeal(h.Spec, h.ActionType),
		RecordID:         recordID,
		RecordTable:      h.Spec.RecordTable,
		AuditTable:       h.Spec.AuditTable,
		AuditIDColumn:    h.Spec.AuditIDColumn,
		ActionType:       strings.ToUpper(strings.TrimSpace(h.ActionType)),
		Amount:           h.Amount,
		SubmittedBy:      h.RequestedBy,
		SubmittedByEmail: h.RequestedBy,
	})
	if err != nil || instID == "" {
		if err != nil {
			api.LogError("[ApprovalEngine] EnsureInstanceForRecord %s/%s: %v", mod, recordID, err)
		}
		return ""
	}
	return instID
}

func liveMatrixAppliesForRecord(ctx context.Context, pool *pgxpool.Pool, moduleCode, recordID string) bool {
	var txType, entity string
	var amount float64
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(transaction_type,''), COALESCE(entity_code,''), COALESCE(amount,0)
		FROM uam.approval_instance
		WHERE record_id=$1 AND ($2='' OR module_code=$2) AND is_deleted=false
		ORDER BY submitted_at DESC LIMIT 1`, recordID, strings.TrimSpace(moduleCode),
	).Scan(&txType, &entity, &amount)
	if err == nil && strings.TrimSpace(txType) != "" {
		mod := strings.TrimSpace(moduleCode)
		if mod == "" {
			mod = "CASH"
		}
		m, mErr := ResolveMatrix(ctx, pool, mod, entity, txType, amount)
		return mErr == nil && m != nil
	}
	h := loadHealContext(ctx, pool, moduleCode, recordID)
	if !h.Found || !isPendingAuditStatus(h.Status) {
		return false
	}
	mod := h.Spec.ModuleCode
	if strings.TrimSpace(moduleCode) != "" {
		mod = moduleCode
	}
	m, mErr := ResolveMatrix(ctx, pool, mod, h.EntityCode, txTypeFromHeal(h.Spec, h.ActionType), h.Amount)
	return mErr == nil && m != nil
}

func applyNoPendingMatrixGate(ctx context.Context, pool *pgxpool.Pool, req ActOnPendingRequest, result *ActOnPendingResult, healed bool) (ActOnPendingResult, error) {
	if !healed {
		if instID := EnsureInstanceForRecord(ctx, pool, req.ModuleCode, req.RecordID); instID != "" {
			return actOnPendingOrDiagnose(ctx, pool, req, true)
		}
	}
	if liveMatrixAppliesForRecord(ctx, pool, req.ModuleCode, req.RecordID) {
		result.Reason = "not your turn in approval sequence"
	}
	return *result, nil
}
