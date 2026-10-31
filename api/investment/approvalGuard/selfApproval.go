package approvalguard

import (
	"context"
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/governance"
)

func Deny(ctx context.Context, w http.ResponseWriter, db api.RowQuerier, query string, args ...any) bool {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		api.LogErrorForResponse(w, "self-approval lookup failed: %v", err)
		api.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to verify approval", "")
		return true
	}
	defer rows.Close()
	makers := make([]string, 0)
	for rows.Next() {
		var maker string
		if err := rows.Scan(&maker); err != nil {
			api.LogErrorForResponse(w, "self-approval scan failed: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to verify approval", "")
			return true
		}
		makers = append(makers, maker)
	}
	if err := rows.Err(); err != nil {
		api.LogErrorForResponse(w, "self-approval rows failed: %v", err)
		api.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to verify approval", "")
		return true
	}
	if governance.IsSelfApprovalCtx(ctx, makers...) {
		api.RespondEnvelopeError(w, http.StatusForbidden, governance.ErrSelfApproval.Error(), "SELF_APPROVAL")
		return true
	}
	return false
}
