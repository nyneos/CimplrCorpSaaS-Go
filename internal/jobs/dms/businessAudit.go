package dmsjobs

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

func truncateStringRunes(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// businessAuditTarget is one candidate table a DMS_TRIGGER row can land in for
// a given module/sub-module. A sub-module maps to a LIST of targets because
// several sub-modules cover more than one underlying record table (e.g. FD
// closure has initiate/confirm/request variants; MF onboarding has separate
// AMC/Scheme/DP/Demat/Folio audit tables). tryInsertBusinessAudit tries each
// target in order for a given source ID and stops at the first one whose
// parent ID actually matches a row.
type businessAuditTarget struct {
	Table     string
	ParentCol string
	ActionCol string // "action_type" (investment) or "actiontype" (cash/fx/most investment onboarding tables)
}

// businessAuditBySub maps every DMS module/sub-module pair that FireDmsEvent
// is ever called with to the audit table(s) a DMS_TRIGGER row belongs in.
//
// Every sub-module that calls dmsjobs.FireDmsEvent MUST have an entry here —
// partial coverage means some modules silently get no DMS_TRIGGER audit trail
// at all while sibling modules do, for no principled reason. When a new
// FireDmsEvent call site is added for a sub-module, add its target(s) here in
// the same change.
var businessAuditBySub = map[string][]businessAuditTarget{
	// ── CASH ──────────────────────────────────────────────────────────────
	"BANK_BALANCE":         {{Table: "public.auditactionbankbalances", ParentCol: "balance_id", ActionCol: "actiontype"}},
	"BANK_STATEMENT":       {{Table: "cimplrcorpsaas.auditactionbankstatement", ParentCol: "bankstatementid", ActionCol: "actiontype"}},
	"BANK_LIMIT":           {{Table: "cimplrcorpsaas.auditactionbanklimit", ParentCol: "limit_id", ActionCol: "action_type"}},
	"LIMIT_UTILIZATION":    {{Table: "cimplrcorpsaas.auditactionbanklimitutilization", ParentCol: "utilization_id", ActionCol: "action_type"}},
	"SWEEP_CONFIG":         {{Table: "cimplrcorpsaas.auditactionsweepconfiguration", ParentCol: "sweep_id", ActionCol: "actiontype"}},
	"SWEEP_INITIATION":     {{Table: "cimplrcorpsaas.auditactionsweepinitiation", ParentCol: "initiation_id", ActionCol: "actiontype"}},
	"FUND_PLANNING":        {{Table: "public.auditaction_fund_plan_groups", ParentCol: "group_id", ActionCol: "actiontype"}},
	"CASH_FLOW_PROJECTION": {{Table: "cimplrcorpsaas.audit_action_cashflow_proposal", ParentCol: "proposal_id", ActionCol: "actiontype"}},
	"PAYABLE_RECEIVABLE": {
		{Table: "public.auditactionpayable", ParentCol: "payable_id", ActionCol: "actiontype"},
		{Table: "public.auditactionreceivable", ParentCol: "receivable_id", ActionCol: "actiontype"},
	},

	// ── FX ────────────────────────────────────────────────────────────────
	"EXPOSURE_CREATION":    {{Table: "public.auditactionexposure", ParentCol: "exposure_header_id", ActionCol: "actiontype"}},
	"EXPOSURE_UPLOAD":      {{Table: "public.auditactionexposure", ParentCol: "exposure_header_id", ActionCol: "actiontype"}},
	"EXPOSURE_BUCKETING":   {{Table: "public.auditactionexposurebucketing", ParentCol: "exposure_header_id", ActionCol: "actiontype"}},
	"FORWARD_BOOKING":      {{Table: "public.auditactionforwardbooking", ParentCol: "system_transaction_id", ActionCol: "actiontype"}},
	"FORWARD_CANCELLATION": {{Table: "public.auditactionforwardcancellation", ParentCol: "booking_id", ActionCol: "actiontype"}},
	"FORWARD_ROLLOVER":     {{Table: "public.auditactionforwardrollover", ParentCol: "booking_id", ActionCol: "actiontype"}},
	"FORWARD_CANCEL_ROLL":  {{Table: "public.auditactionforwardcancellation", ParentCol: "booking_id", ActionCol: "actiontype"}},
	"FORWARD_MTM":          {{Table: "public.auditactionforwardmtm", ParentCol: "booking_id", ActionCol: "actiontype"}},
	"HEDGE_LINK":           {{Table: "public.auditactionhedgelink", ParentCol: "exposure_header_id", ActionCol: "actiontype"}},

	// ── FD ────────────────────────────────────────────────────────────────
	"FD_BOOKING":       {{Table: "investment.fd_audit_booking_request", ParentCol: "booking_id", ActionCol: "action_type"}},
	"FD_CONFIRMATION":  {{Table: "investment.fd_audit_confirmation", ParentCol: "confirmation_id", ActionCol: "action_type"}},
	"FD_MASTER":        {{Table: "investment.fd_audit_master", ParentCol: "fd_id", ActionCol: "action_type"}},
	"FD_CASHFLOW":      {{Table: "investment.fd_audit_cashflow_schedule", ParentCol: "audit_id", ActionCol: "action_type"}},
	"FD_ACCRUAL":       {{Table: "investment.fd_accrual_run_audit", ParentCol: "run_id", ActionCol: "action_type"}},
	"FD_ACCRUAL_SCHED": {{Table: "investment.fd_accrual_schedule_config_audit", ParentCol: "config_id", ActionCol: "action_type"}},
	"FD_RECEIPT":       {{Table: "investment.fd_interest_receipt_audit", ParentCol: "receipt_id", ActionCol: "action_type"}},
	"FD_EXCEPTION":     {{Table: "investment.fd_receipt_exception_audit", ParentCol: "exception_id", ActionCol: "action_type"}},
	"FD_TDS_REGISTER":  {{Table: "investment.fd_tds_receipt_audit", ParentCol: "tds_id", ActionCol: "action_type"}},
	// FD closure spans three record shapes (initiate / confirm / the older
	// combined closure_request) sharing one DMS sub-module code — try each.
	"FD_CLOSURE": {
		{Table: "cimplr.fd_closure_initiate_audit", ParentCol: "closure_initiate_id", ActionCol: "action_type"},
		{Table: "cimplr.fd_closure_confirm_audit", ParentCol: "closure_confirm_id", ActionCol: "action_type"},
		{Table: "investment.fd_audit_closure_request", ParentCol: "closure_request_id", ActionCol: "action_type"},
	},

	// ── Mutual Fund ───────────────────────────────────────────────────────
	"MF_PROPOSAL":        {{Table: "investment.auditactionproposal", ParentCol: "proposal_id", ActionCol: "actiontype"}},
	"MF_PORTFOLIO":       {{Table: "investment.auditactionproposal", ParentCol: "proposal_id", ActionCol: "actiontype"}},
	"MF_INITIATION":      {{Table: "investment.auditactioninitiation", ParentCol: "initiation_id", ActionCol: "actiontype"}},
	"MF_CONFIRMATION":    {{Table: "investment.auditactioninvestmentconfirmation", ParentCol: "confirmation_id", ActionCol: "actiontype"}},
	"MF_REDEMPTION":      {{Table: "investment.auditactionredemption", ParentCol: "redemption_id", ActionCol: "actiontype"}},
	"MF_REDEMPTION_CONF": {{Table: "investment.auditactionredemptionconfirmation", ParentCol: "redemption_confirm_id", ActionCol: "actiontype"}},
	// All accounting-workbench activity types (corporate action, dividend,
	// FVO, MTM) share one audit table keyed by activity_id.
	"MF_ACCOUNTING": {{Table: "investment.auditactionaccountingactivity", ParentCol: "activity_id", ActionCol: "actiontype"}},
	// Onboarding covers five distinct master types under one sub-module code;
	// a POST_UPLOAD batch id won't match any of these and is a legitimate no-op.
	"MF_ONBOARD": {
		{Table: "investment.auditactionamc", ParentCol: "amc_id", ActionCol: "actiontype"},
		{Table: "investment.auditactionscheme", ParentCol: "scheme_id", ActionCol: "actiontype"},
		{Table: "investment.auditactiondp", ParentCol: "dp_id", ActionCol: "actiontype"},
		{Table: "investment.auditactiondemat", ParentCol: "demat_id", ActionCol: "actiontype"},
		{Table: "investment.auditactionfolio", ParentCol: "folio_id", ActionCol: "actiontype"},
	},
}

type businessAuditOutcome struct {
	Trigger   string
	RuleCount int
	OKCount   int
	FailCount int
	RunIDs    []string
	LastError string
}

// recordBusinessDmsAudits writes one DMS_TRIGGER row per source_id into the
// module's main auditaction / fd_audit_* table (best-effort; never fails the
// run). For sub-modules with more than one candidate target table, tries each
// in turn and stops at the first one whose parent ID actually matches a row.
func recordBusinessDmsAudits(
	ctx context.Context,
	pool *pgxpool.Pool,
	moduleCode, subModuleCode string,
	sourceIDs []string,
	actor string,
	out businessAuditOutcome,
) {
	if pool == nil || len(sourceIDs) == 0 {
		return
	}
	sub := strings.TrimSpace(subModuleCode)
	// Status here is the DMS *run* outcome (doc gen succeeded/failed), NOT maker-checker
	// approval of the business record. List endpoints must ignore DMS_TRIGGER when
	// resolving processing_status (see fd booking/confirmation latest_audit filters).
	status := "APPROVED"
	if out.FailCount > 0 && out.OKCount == 0 {
		status = "REJECTED"
	} else if out.FailCount > 0 {
		status = "APPROVED" // partial — still show as completed with reason
	}
	reason := fmt.Sprintf(
		"DMS %s · rules=%d ok=%d fail=%d",
		out.Trigger, out.RuleCount, out.OKCount, out.FailCount,
	)
	if len(out.RunIDs) > 0 {
		reason += " · runs=" + strings.Join(out.RunIDs, ",")
	}
	if out.LastError != "" {
		reason += " · err=" + truncateStringRunes(out.LastError, 180)
	}
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "dms-event"
	}

	targets, ok := businessAuditBySub[sub]
	if !ok || len(targets) == 0 {
		api.LogInfo("[DMS-EVENT] no business audit mapping for module=%s sub=%s", moduleCode, sub)
		return
	}
	for _, id := range sourceIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		matched := false
		for _, tgt := range targets {
			actionCol := tgt.ActionCol
			if actionCol == "" {
				actionCol = "actiontype"
			}
			if tryInsertBusinessAudit(ctx, pool, businessAuditInsert{
				Table: tgt.Table, ParentCol: tgt.ParentCol, ActionCol: actionCol,
				ParentID: id, Status: status, Reason: reason, Actor: actor,
			}) {
				matched = true
				break
			}
		}
		if !matched {
			api.LogInfo("[DMS-EVENT] no matching business audit target for module=%s sub=%s id=%s", moduleCode, sub, id)
		}
	}
}

// businessAuditInsert describes one DMS_TRIGGER row to append to a module's
// auditaction* / fd_audit_* table: where it goes (table + column names) and
// what it says (parent id, status, reason, actor).
type businessAuditInsert struct {
	Table     string
	ParentCol string
	ActionCol string
	ParentID  string
	Status    string
	Reason    string
	Actor     string
}

// tryInsertBusinessAudit inserts a DMS_TRIGGER row scoped to a parent ID that
// must actually exist in the target table, so a wrong candidate table in a
// multi-target list is silently skipped rather than inserting an orphan row.
func tryInsertBusinessAudit(ctx context.Context, pool *pgxpool.Pool, in businessAuditInsert) bool {
	// Table/column names come from the fixed businessAuditBySub map, never
	// from request input, so building the query with fmt.Sprintf is safe here.
	// Minimal columns shared across most auditaction* / fd_audit_* tables.
	q := fmt.Sprintf(`
		INSERT INTO %s (%s, %s, processing_status, reason, requested_by, requested_at)
		SELECT $1, 'DMS_TRIGGER', $2, $3, $4, now()
		WHERE EXISTS (SELECT 1 FROM %s WHERE %s = $1)`,
		in.Table, in.ParentCol, in.ActionCol, in.Table, in.ParentCol)
	tag, err := pool.Exec(ctx, q, in.ParentID, in.Status, in.Reason, in.Actor)
	if err != nil {
		// Fallback without reason (some tables may lack it).
		q2 := fmt.Sprintf(`
			INSERT INTO %s (%s, %s, processing_status, requested_by, requested_at)
			SELECT $1, 'DMS_TRIGGER', $2, $3, now()
			WHERE EXISTS (SELECT 1 FROM %s WHERE %s = $1)`,
			in.Table, in.ParentCol, in.ActionCol, in.Table, in.ParentCol)
		tag2, err2 := pool.Exec(ctx, q2, in.ParentID, in.Status, in.Actor)
		if err2 != nil {
			api.LogError("[DMS-EVENT] business audit insert table=%s parent=%s: %v (fallback: %v)",
				in.Table, in.ParentID, err, err2)
			return false
		}
		return tag2.RowsAffected() > 0
	}
	return tag.RowsAffected() > 0
}
