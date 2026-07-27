package policy

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/services/policysvc"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandlePelValidate proxies PEL syntax checks to the Policy Service
// (POST /v1/pel/validate) so the form writer can validate before submit.
func HandlePelValidate(pool *pgxpool.Pool) http.HandlerFunc {
	_ = pool
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req struct {
			Expression string `json:"expression"`
			ReturnType string `json:"return_type"`
		}
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		expr := strings.TrimSpace(req.Expression)
		if expr == "" {
			api.RespondEnvelopeSuccess(w, "PEL validation", map[string]interface{}{
				"valid":   false,
				"message": "expression is empty",
			})
			return
		}
		client := policysvc.NewFromEnv()
		out, err := client.ValidatePEL(r.Context(), expr, req.ReturnType)
		if err != nil {
			api.LogErrorForResponse(w, "pel validate relay: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadGateway, "Policy Service unreachable for PEL validate — start local :8184", "PEL_VALIDATE_UNAVAILABLE")
			return
		}
		api.RespondEnvelopeSuccess(w, "PEL validation", map[string]interface{}{
			"valid":   out.Valid,
			"message": out.Message,
		})
	}
}

// validatePELOnWrite rejects create/update when formula / additional PEL won't compile.
// Empty additional expression is allowed (optional filter).
func validatePELOnWrite(ctx context.Context, ruleType, formulaExpr, formulaReturn, addlExpr string) error {
	client := policysvc.NewFromEnv()
	check := func(label, expr, returnType string) error {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			return nil
		}
		out, err := client.ValidatePEL(ctx, expr, returnType)
		if err != nil {
			return fmt.Errorf("%s: Policy Service unreachable for PEL validate (%v)", label, err)
		}
		if out == nil || !out.Valid {
			msg := "invalid expression"
			if out != nil && strings.TrimSpace(out.Message) != "" {
				msg = out.Message
			}
			return fmt.Errorf("%s: %s", label, msg)
		}
		return nil
	}

	if strings.EqualFold(strings.TrimSpace(ruleType), "formula") {
		ret := strings.TrimSpace(formulaReturn)
		if ret == "" {
			ret = "boolean"
		}
		if err := check("formula", formulaExpr, ret); err != nil {
			return err
		}
	}
	if err := check("additional condition", addlExpr, "boolean"); err != nil {
		return err
	}
	return nil
}
