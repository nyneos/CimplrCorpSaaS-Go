package approvalengine

import (
	"CimplrCorpSaas/api"
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GateOutcome is the decision a business handler needs before mutating a record
// that may be under an approval matrix.
//
//	Acted     — this actor's eye was recorded.
//	Finalized — the instance resolved on that action, so the handler may now
//	            apply its business mutation. When Acted is true and Finalized is
//	            false, other eyes are still outstanding and the record MUST be
//	            left in its pending state.
//	Blocked   — a pending instance exists but this actor has no active eye on
//	            it (wrong turn in a sequential matrix, or not an approver at
//	            all). The handler must refuse the request with Reason.
//
// The zero value means the engine is not in play for this record (no matrix, no
// pending instance, or a stale one that was cancelled), and the caller should
// fall through to its legacy non-matrix path.
type GateOutcome struct {
	Acted     bool
	Finalized bool
	Blocked   bool
	Reason    string
}

// Gate records an approve/reject decision against a record's pending approval
// instance and reports what the caller is allowed to do next.
//
// It exists because ActOnPendingOrDiagnose alone is not enough: callers that
// only check res.Acted silently fall through to their legacy mutation when the
// actor is out of sequence, which lets the wrong approver complete a
// transaction and lets a first-of-two approval finish it outright.
func Gate(ctx context.Context, pool *pgxpool.Pool, req ActOnPendingRequest) GateOutcome {
	res, err := ActOnPendingOrDiagnose(ctx, pool, req)
	if err != nil {
		api.LogError("[ApprovalEngine] gate failed module=%s record=%s: %v", req.ModuleCode, req.RecordID, err)
		return GateOutcome{}
	}
	if res.Acted {
		return GateOutcome{Acted: true, Finalized: res.InstanceStatus != InstStatusPending}
	}
	if res.CancelledStale {
		return GateOutcome{}
	}
	if reason := strings.TrimSpace(res.Reason); reason != "" {
		return GateOutcome{Blocked: true, Reason: reason}
	}
	if res.Diagnosis.HasPending {
		return GateOutcome{Blocked: true, Reason: "not your turn in the configured approval sequence"}
	}
	return GateOutcome{}
}
