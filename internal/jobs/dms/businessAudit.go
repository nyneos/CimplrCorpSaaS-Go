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

// businessAuditTarget maps a DMS module/sub-module to the business audit trail
// table where a single DMS_TRIGGER row is appended after event-driven generation.
type businessAuditTarget struct {
	Table     string
	ParentCol string
	ActionCol string // "action_type" (investment) or "actiontype" (cash/fx)
}

// One entry per source row when FireDmsEvent actually runs matching rules.
var businessAuditBySub = map[string]businessAuditTarget{
	// CASH
	"BANK_BALANCE":       {Table: "public.auditactionbankbalances", ParentCol: "balance_id", ActionCol: "actiontype"},
	"BANK_STATEMENT":     {Table: "cimplrcorpsaas.auditactionbankstatement", ParentCol: "bankstatementid", ActionCol: "actiontype"},
	"PAYABLE_RECEIVABLE": {Table: "", ParentCol: "", ActionCol: "actiontype"}, // split payable/receivable below
	// FX
	"EXPOSURE_CREATION":    {Table: "public.auditactionexposure", ParentCol: "exposure_header_id", ActionCol: "actiontype"},
	"EXPOSURE_UPLOAD":      {Table: "public.auditactionexposure", ParentCol: "exposure_header_id", ActionCol: "actiontype"},
	"EXPOSURE_BUCKETING":   {Table: "public.auditactionexposurebucketing", ParentCol: "exposure_header_id", ActionCol: "actiontype"},
	"FORWARD_BOOKING":      {Table: "public.auditactionforwardbooking", ParentCol: "system_transaction_id", ActionCol: "actiontype"},
	"FORWARD_CANCELLATION": {Table: "public.auditactionforwardcancellation", ParentCol: "booking_id", ActionCol: "actiontype"},
	"FORWARD_ROLLOVER":     {Table: "public.auditactionforwardrollover", ParentCol: "booking_id", ActionCol: "actiontype"},
	"FORWARD_CANCEL_ROLL":  {Table: "public.auditactionforwardcancellation", ParentCol: "booking_id", ActionCol: "actiontype"},
	"FORWARD_MTM":          {Table: "public.auditactionforwardmtm", ParentCol: "booking_id", ActionCol: "actiontype"},
	"HEDGE_LINK":           {Table: "public.auditactionhedgelink", ParentCol: "exposure_header_id", ActionCol: "actiontype"},
	// FD — verified tables
	"FD_BOOKING":      {Table: "investment.fd_audit_booking_request", ParentCol: "booking_id", ActionCol: "action_type"},
	"FD_CONFIRMATION": {Table: "investment.fd_audit_confirmation", ParentCol: "confirmation_id", ActionCol: "action_type"},
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
// module's main auditaction / fd_audit_* table (best-effort; never fails the run).
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

	if sub == "PAYABLE_RECEIVABLE" {
		for _, id := range sourceIDs {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			// Try payable then receivable — only one should match.
			if tryInsertBusinessAudit(ctx, pool,
				"public.auditactionpayable", "payable_id", "actiontype",
				id, status, reason, actor) {
				continue
			}
			_ = tryInsertBusinessAudit(ctx, pool,
				"public.auditactionreceivable", "receivable_id", "actiontype",
				id, status, reason, actor)
		}
		return
	}

	tgt, ok := businessAuditBySub[sub]
	if !ok || tgt.Table == "" || tgt.ParentCol == "" {
		api.LogInfo("[DMS-EVENT] no business audit mapping for module=%s sub=%s", moduleCode, sub)
		return
	}
	actionCol := tgt.ActionCol
	if actionCol == "" {
		actionCol = "actiontype"
	}
	for _, id := range sourceIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		_ = tryInsertBusinessAudit(ctx, pool, tgt.Table, tgt.ParentCol, actionCol, id, status, reason, actor)
	}
}

func tryInsertBusinessAudit(
	ctx context.Context,
	pool *pgxpool.Pool,
	table, parentCol, actionCol, parentID, status, reason, actor string,
) bool {
	// Minimal columns shared across most auditaction* / fd_audit_* tables.
	q := fmt.Sprintf(`
		INSERT INTO %s (%s, %s, processing_status, reason, requested_by, requested_at)
		VALUES ($1, 'DMS_TRIGGER', $2, $3, $4, now())`,
		table, parentCol, actionCol)
	_, err := pool.Exec(ctx, q, parentID, status, reason, actor)
	if err != nil {
		// Fallback without reason (some tables may lack it).
		q2 := fmt.Sprintf(`
			INSERT INTO %s (%s, %s, processing_status, requested_by, requested_at)
			VALUES ($1, 'DMS_TRIGGER', $2, $3, now())`,
			table, parentCol, actionCol)
		if _, err2 := pool.Exec(ctx, q2, parentID, status, actor); err2 != nil {
			api.LogError("[DMS-EVENT] business audit insert table=%s parent=%s: %v (fallback: %v)",
				table, parentID, err, err2)
			return false
		}
	}
	return true
}
