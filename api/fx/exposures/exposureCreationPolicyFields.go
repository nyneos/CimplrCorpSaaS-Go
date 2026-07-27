package exposures

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ExposureCreationRow is the canonical business-field shape for
// EXPOSURE_CREATION — one field per real scalar column on public.exposure_headers
// (jsonb additional_header_details excluded; created_at/updated_at/deleted_at/
// deleted_by/upload_link/upload_s3_key/file_hash/batch_id/time_based skipped as
// pure upload/audit plumbing, not business fields a policy would evaluate),
// plus LineItemAmount — a SUM(exposure_line_items.line_item_amount) aggregate
// for the header, since EXPOSURE_CREATION's real mutation handlers all act at
// header level (soft delete / approval-status transition on exposure_headers),
// never on an individual line item.
//
// Exported (unlike every other sub-module's row type in this rollout) because
// both api/fx/exposures and api/fx/v91 declare "package exposures" as two
// independent Go packages (different import paths) that both mutate
// exposure_headers under this one sub_module_code — v91's fbActions.go/
// fNUploadV2.go import this file's exported Build/Load functions rather than
// hand-rolling a second, divergent field set.
//
// Before this file: BulkUpdateValueDates, BulkApproveExposures,
// BulkRejectExposures, BulkDeleteExposures (api/fx/v91/fbActions.go) and
// DeleteExposureHeaders, RejectMultipleExposureHeaders,
// ApproveMultipleExposureHeaders (api/fx/exposures/expUpload.go) each passed
// only exposure_header_id (BulkUpdateValueDates also passed new_value_date) —
// see database/2026-07-27.sql for the full audit.
type ExposureCreationRow struct {
	ExposureHeaderID       string
	CompanyCode            string
	Entity                 string
	Entity1                string
	Entity2                string
	Entity3                string
	ExposureType           string
	DocumentID             string
	DocumentDate           string
	CounterpartyType       string
	CounterpartyCode       string
	CounterpartyName       string
	Currency               string
	TotalOriginalAmount    float64
	TotalOpenAmount        float64
	ValueDate              string
	Status                 string
	IsActive               bool
	ApprovalStatus         string
	ApprovalComment        string
	ApprovedBy             string
	DeleteComment          string
	RequestedBy            string
	RejectionComment       string
	ApprovedAt             string
	RejectedBy             string
	RejectedAt             string
	AmountInLocalCurrency  float64
	PostingDate            string
	Text                   string
	GLAccount              string
	Reference              string
	ExposureCategory       string
	ExposureCreationStatus string
	IsDeleted              bool
	LineItemAmount         float64
}

// BuildExposureCreationPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for EXPOSURE_CREATION (see
// cmd/seedDomainCatalog/exposureCreationCanonical.go).
func BuildExposureCreationPolicyFields(row ExposureCreationRow) map[string]interface{} {
	return map[string]interface{}{
		"exposure_header_id":       row.ExposureHeaderID,
		"company_code":             row.CompanyCode,
		"entity":                   row.Entity,
		"entity1":                  row.Entity1,
		"entity2":                  row.Entity2,
		"entity3":                  row.Entity3,
		"exposure_type":            row.ExposureType,
		"document_id":              row.DocumentID,
		"document_date":            row.DocumentDate,
		"counterparty_type":        row.CounterpartyType,
		"counterparty_code":        row.CounterpartyCode,
		"counterparty_name":        row.CounterpartyName,
		"currency":                 row.Currency,
		"total_original_amount":    row.TotalOriginalAmount,
		"total_open_amount":        row.TotalOpenAmount,
		"value_date":               row.ValueDate,
		"status":                   row.Status,
		"is_active":                row.IsActive,
		"approval_status":          row.ApprovalStatus,
		"approval_comment":         row.ApprovalComment,
		"approved_by":              row.ApprovedBy,
		"delete_comment":           row.DeleteComment,
		"requested_by":             row.RequestedBy,
		"rejection_comment":        row.RejectionComment,
		"approved_at":              row.ApprovedAt,
		"rejected_by":              row.RejectedBy,
		"rejected_at":              row.RejectedAt,
		"amount_in_local_currency": row.AmountInLocalCurrency,
		"posting_date":             row.PostingDate,
		"text":                     row.Text,
		"gl_account":               row.GLAccount,
		"reference":                row.Reference,
		"exposure_category":        row.ExposureCategory,
		"exposure_creation_status": row.ExposureCreationStatus,
		"is_deleted":               row.IsDeleted,
		"line_item_amount":         row.LineItemAmount,
	}
}

// LoadExposureCreationRow fetches the full canonical row by exposure_header_id
// — used by every EXPOSURE_CREATION handler here and in api/fx/v91, all of
// which only ever receive an exposure_header_id in the request, never the
// full business row.
func LoadExposureCreationRow(ctx context.Context, pool *pgxpool.Pool, exposureHeaderID string) (ExposureCreationRow, error) {
	var row ExposureCreationRow
	row.ExposureHeaderID = exposureHeaderID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(h.company_code,''), COALESCE(h.entity,''), COALESCE(h.entity1,''), COALESCE(h.entity2,''), COALESCE(h.entity3,''),
		       COALESCE(h.exposure_type,''), COALESCE(h.document_id,''), COALESCE(h.document_date::text,''),
		       COALESCE(h.counterparty_type,''), COALESCE(h.counterparty_code,''), COALESCE(h.counterparty_name,''), COALESCE(h.currency,''),
		       COALESCE(h.total_original_amount,0), COALESCE(h.total_open_amount,0), COALESCE(h.value_date::text,''),
		       COALESCE(h.status,''), COALESCE(h.is_active,false),
		       COALESCE(h.approval_status,''), COALESCE(h.approval_comment,''), COALESCE(h.approved_by,''),
		       COALESCE(h.delete_comment,''), COALESCE(h.requested_by,''), COALESCE(h.rejection_comment,''),
		       COALESCE(h.approved_at::text,''), COALESCE(h.rejected_by,''), COALESCE(h.rejected_at::text,''),
		       COALESCE(h.amount_in_local_currency,0), COALESCE(h.posting_date::text,''), COALESCE(h.text,''),
		       COALESCE(h.gl_account,''), COALESCE(h.reference,''), COALESCE(h.exposure_category,''),
		       COALESCE(h.exposure_creation_status,''), COALESCE(h.is_deleted,false),
		       COALESCE((SELECT SUM(l.line_item_amount) FROM exposure_line_items l WHERE l.exposure_header_id = h.exposure_header_id), 0)
		FROM exposure_headers h
		WHERE h.exposure_header_id = $1`, exposureHeaderID,
	).Scan(
		&row.CompanyCode, &row.Entity, &row.Entity1, &row.Entity2, &row.Entity3,
		&row.ExposureType, &row.DocumentID, &row.DocumentDate,
		&row.CounterpartyType, &row.CounterpartyCode, &row.CounterpartyName, &row.Currency,
		&row.TotalOriginalAmount, &row.TotalOpenAmount, &row.ValueDate,
		&row.Status, &row.IsActive,
		&row.ApprovalStatus, &row.ApprovalComment, &row.ApprovedBy,
		&row.DeleteComment, &row.RequestedBy, &row.RejectionComment,
		&row.ApprovedAt, &row.RejectedBy, &row.RejectedAt,
		&row.AmountInLocalCurrency, &row.PostingDate, &row.Text,
		&row.GLAccount, &row.Reference, &row.ExposureCategory,
		&row.ExposureCreationStatus, &row.IsDeleted,
		&row.LineItemAmount,
	)
	if err != nil {
		return row, fmt.Errorf("load exposure creation row for policy: %w", err)
	}
	return row, nil
}

// ApplyExposureCreationEdits overlays a partial edit map onto an
// already-loaded canonical row, so the policy check sees the full row
// as-it-will-be-after-the-edit, not just the touched keys. Only
// BulkUpdateValueDates (api/fx/v91/fbActions.go) currently performs a
// partial-field edit against this sub-module (value_date only).
func ApplyExposureCreationEdits(row ExposureCreationRow, edits map[string]interface{}) ExposureCreationRow {
	str := func(v interface{}) (string, bool) {
		switch t := v.(type) {
		case string:
			return t, true
		case fmt.Stringer:
			return t.String(), true
		default:
			if v == nil {
				return "", false
			}
			return fmt.Sprint(v), true
		}
	}
	num := func(v interface{}) (float64, bool) {
		switch t := v.(type) {
		case float64:
			return t, true
		case float32:
			return float64(t), true
		case int:
			return float64(t), true
		case int64:
			return float64(t), true
		case json.Number:
			f, err := t.Float64()
			return f, err == nil
		case string:
			f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
			return f, err == nil
		default:
			return 0, false
		}
	}
	for k, v := range edits {
		switch k {
		case "company_code":
			if s, ok := str(v); ok {
				row.CompanyCode = s
			}
		case "entity":
			if s, ok := str(v); ok {
				row.Entity = s
			}
		case "entity1":
			if s, ok := str(v); ok {
				row.Entity1 = s
			}
		case "entity2":
			if s, ok := str(v); ok {
				row.Entity2 = s
			}
		case "entity3":
			if s, ok := str(v); ok {
				row.Entity3 = s
			}
		case "exposure_type":
			if s, ok := str(v); ok {
				row.ExposureType = s
			}
		case "document_id":
			if s, ok := str(v); ok {
				row.DocumentID = s
			}
		case "document_date":
			if s, ok := str(v); ok {
				row.DocumentDate = s
			}
		case "counterparty_type":
			if s, ok := str(v); ok {
				row.CounterpartyType = s
			}
		case "counterparty_code":
			if s, ok := str(v); ok {
				row.CounterpartyCode = s
			}
		case "counterparty_name":
			if s, ok := str(v); ok {
				row.CounterpartyName = s
			}
		case "currency":
			if s, ok := str(v); ok {
				row.Currency = s
			}
		case "total_original_amount":
			if f, ok := num(v); ok {
				row.TotalOriginalAmount = f
			}
		case "total_open_amount":
			if f, ok := num(v); ok {
				row.TotalOpenAmount = f
			}
		case "value_date":
			if s, ok := str(v); ok {
				row.ValueDate = s
			}
		case "status":
			if s, ok := str(v); ok {
				row.Status = s
			}
		case "approval_status":
			if s, ok := str(v); ok {
				row.ApprovalStatus = s
			}
		case "amount_in_local_currency":
			if f, ok := num(v); ok {
				row.AmountInLocalCurrency = f
			}
		case "posting_date":
			if s, ok := str(v); ok {
				row.PostingDate = s
			}
		case "text":
			if s, ok := str(v); ok {
				row.Text = s
			}
		case "gl_account":
			if s, ok := str(v); ok {
				row.GLAccount = s
			}
		case "reference":
			if s, ok := str(v); ok {
				row.Reference = s
			}
		case "exposure_category":
			if s, ok := str(v); ok {
				row.ExposureCategory = s
			}
		case "exposure_creation_status":
			if s, ok := str(v); ok {
				row.ExposureCreationStatus = s
			}
		}
	}
	return row
}
