package bankstatement

import (
	"context"
	"net/http"
	"strconv"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const smartCatReviewAPIPath = "/cash/smart-cat/review-action"

type smartCatReviewRow struct {
	BankStatementID string
	EntityID        string
	AccountNumber   string
	Confidence      *float64
	CategoryName    string
}

func loadSmartCatReviewRow(ctx context.Context, pool *pgxpool.Pool, txnID int64, categoryID string) (smartCatReviewRow, error) {
	var row smartCatReviewRow
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(t.bank_statement_id::text, ''),
		       COALESCE(bs.entity_id, ''),
		       COALESCE(bs.account_number, ''),
		       t.confidence_score::FLOAT8,
		       COALESCE(mc.category_name, '')
		FROM cimplrcorpsaas.bank_statement_transactions t
		LEFT JOIN cimplrcorpsaas.bank_statements bs
		       ON bs.bank_statement_id = t.bank_statement_id
		LEFT JOIN public.mastercashflowcategory mc
		       ON mc.category_id::text = $2
		WHERE t.transaction_id = $1`, txnID, categoryID,
	).Scan(&row.BankStatementID, &row.EntityID, &row.AccountNumber, &row.Confidence, &row.CategoryName)
	return row, err
}

func buildSmartCatReviewPolicyFields(row smartCatReviewRow) map[string]interface{} {
	return map[string]interface{}{
		"bank_statement_id": row.BankStatementID,
		"entity_id":         row.EntityID,
		"account_number":    row.AccountNumber,
		"confidence":        row.Confidence,
		"category_name":     row.CategoryName,
	}
}

// smartCatReviewPolicyParams groups the review-action-specific inputs for
// enforceSmartCatReview so the function itself only needs the shared
// infra parameters (ctx, w, r, pool) plus this one struct.
type smartCatReviewPolicyParams struct {
	TxnID        int64
	CategoryID   string
	EventCode    string
	Actor        string
	BlockMessage string
}

func enforceSmartCatReview(ctx context.Context, w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, params smartCatReviewPolicyParams) bool {
	row, err := loadSmartCatReviewRow(ctx, pool, params.TxnID, params.CategoryID)
	if err != nil {
		writeErrJSON(w, "policy check failed — could not load transaction", http.StatusInternalServerError)
		return false
	}
	return runtime.Enforce(ctx, w, r, pool, runtime.EnforceInput{
		EventCode:           params.EventCode,
		ModuleCode:          common.ModuleCash,
		SubModule:           "SMART_CATEGORIZATION",
		EntityCode:          row.EntityID,
		ActorUserID:         params.Actor,
		HandlerName:         "ReviewActionHandler",
		APIPath:             smartCatReviewAPIPath,
		BusinessRecordID:    strconv.FormatInt(params.TxnID, 10),
		BusinessRecordType:  "BANK_STATEMENT_TRANSACTION",
		Fields:              buildSmartCatReviewPolicyFields(row),
		DefaultBlockMessage: params.BlockMessage,
	})
}
