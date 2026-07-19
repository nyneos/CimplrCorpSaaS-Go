package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ExposureRow is a flexible map for template TABLE_HTML / FILTER usage.
type ExposureRow map[string]interface{}

// ExposureUploadPayload holds batch upload context for rich notification templates.
type ExposureUploadPayload struct {
	BatchID         string
	FileName        string
	ExposureCount   int
	TotalOpenAmount float64
	UploadedBy      string
	EntityName      string
	Exposures       []ExposureRow
}

func (p ExposureUploadPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"BatchID":         p.BatchID,
		"FileName":        p.FileName,
		"ExposureCount":   p.ExposureCount,
		"TotalOpenAmount": p.TotalOpenAmount,
		"UploadedBy":      p.UploadedBy,
		"EntityName":      p.EntityName,
		"Exposures":       rowsOrEmpty(p.Exposures),
		"Action":          ActionUpload,
		"ActionAt":        time.Now().Format(time.RFC3339),
	}
}

// ExposureBulkActionPayload holds bulk/single exposure action context.
type ExposureBulkActionPayload struct {
	Action         string
	ExposureIDs    []string
	ApprovedIDs    []string
	RejectedIDs    []string
	DeletedIDs     []string
	Count          int
	RequestedBy    string
	CheckerComment string
	Exposures      []ExposureRow
}

func (p ExposureBulkActionPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":         p.Action,
		"ExposureIDs":    p.ExposureIDs,
		"ApprovedIDs":    p.ApprovedIDs,
		"RejectedIDs":    p.RejectedIDs,
		"DeletedIDs":     p.DeletedIDs,
		"Count":          p.Count,
		"RequestedBy":    p.RequestedBy,
		"CheckerComment": p.CheckerComment,
		"Exposures":      rowsOrEmpty(p.Exposures),
		"ActionAt":       time.Now().Format(time.RFC3339),
	}
}

func BuildExposureUploadPayload(ctx context.Context, pool *pgxpool.Pool, batchID, uploadedBy string) ExposureUploadPayload {
	p := ExposureUploadPayload{
		BatchID:    batchID,
		UploadedBy: uploadedBy,
		Exposures:  []ExposureRow{},
	}
	if pool == nil || strings.TrimSpace(batchID) == "" {
		return p
	}

	var fileName, entityName pgtype.Text
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(file_name,''), COALESCE(ingestion_source,'')
		FROM public.staging_batches_exposures
		WHERE batch_id = $1
	`, batchID).Scan(&fileName, &entityName)
	if fileName.Valid {
		p.FileName = fileName.String
	}

	rows, err := fetchExposureRows(ctx, pool, `h.batch_id = $1`, batchID)
	if err != nil {
		fmt.Printf("[ERROR] BuildExposureUploadPayload batch=%s: %v\n", batchID, err)
		return p
	}
	p.Exposures = rows
	p.ExposureCount = len(rows)
	for _, row := range rows {
		p.TotalOpenAmount += anyToFloat64(row["total_open_amount"])
		if p.EntityName == "" {
			p.EntityName = strField(row, "entity")
		}
	}
	return p
}

// ExposureBulkActionInput groups the fields needed to build a bulk-action
// notification payload, keeping the function signature under the project's
// parameter-count limit.
type ExposureBulkActionInput struct {
	ExposureIDs    []string
	Action         string
	RequestedBy    string
	CheckerComment string
	ApprovedIDs    []string
	RejectedIDs    []string
	DeletedIDs     []string
}

func BuildExposureBulkActionPayload(ctx context.Context, pool *pgxpool.Pool, in ExposureBulkActionInput) ExposureBulkActionPayload {
	p := ExposureBulkActionPayload{
		Action:         in.Action,
		ExposureIDs:    in.ExposureIDs,
		ApprovedIDs:    in.ApprovedIDs,
		RejectedIDs:    in.RejectedIDs,
		DeletedIDs:     in.DeletedIDs,
		Count:          len(in.ExposureIDs),
		RequestedBy:    in.RequestedBy,
		CheckerComment: in.CheckerComment,
		Exposures:      []ExposureRow{},
	}
	if pool == nil || len(in.ExposureIDs) == 0 {
		return p
	}

	rows, err := fetchExposureRows(ctx, pool, `h.exposure_header_id::text = ANY($1)`, in.ExposureIDs)
	if err != nil {
		fmt.Printf("[ERROR] BuildExposureBulkActionPayload action=%s ids=%v: %v\n", in.Action, in.ExposureIDs, err)
		return p
	}
	p.Exposures = rows
	return p
}

const exposureSelectSQL = `
	SELECT
		h.exposure_header_id::text,
		COALESCE(h.company_code, '') AS company_code,
		COALESCE(h.entity, '') AS entity,
		COALESCE(h.document_id, '') AS document_id,
		h.document_date,
		h.posting_date,
		h.value_date,
		COALESCE(h.exposure_type, '') AS exposure_type,
		COALESCE(h.exposure_category, '') AS exposure_category,
		COALESCE(h.counterparty_code, '') AS counterparty_code,
		COALESCE(h.counterparty_name, '') AS counterparty_name,
		COALESCE(h.currency, '') AS currency,
		COALESCE(h.total_original_amount, 0) AS total_original_amount,
		COALESCE(h.total_open_amount, 0) AS total_open_amount,
		COALESCE(h.status, '') AS status,
		COALESCE(h.approval_status, h.exposure_creation_status, '') AS approval_status,
		COALESCE(h.exposure_creation_status, '') AS exposure_creation_status,
		COALESCE(h.approved_by, '') AS approved_by,
		h.approved_at,
		COALESCE(h.rejected_by, '') AS rejected_by,
		h.rejected_at,
		COALESCE(h.rejection_comment, '') AS rejection_comment,
		COALESCE(h.delete_comment, '') AS delete_comment,
		COALESCE(h.requested_by, '') AS requested_by,
		h.batch_id::text AS batch_id,
		h.created_at,
		h.updated_at
	FROM public.exposure_headers h
	WHERE COALESCE(h.is_deleted, false) = false
	  AND `

func fetchExposureRows(ctx context.Context, pool *pgxpool.Pool, whereClause string, arg interface{}) ([]ExposureRow, error) {
	q := exposureSelectSQL + whereClause + ` ORDER BY h.document_id`
	dbRows, err := pool.Query(ctx, q, arg)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()

	var out []ExposureRow
	for dbRows.Next() {
		vals, err := dbRows.Values()
		if err != nil {
			continue
		}
		cols := dbRows.FieldDescriptions()
		row := make(ExposureRow)
		for i, col := range cols {
			row[string(col.Name)] = normalizeExposureVal(vals[i])
		}
		out = append(out, row)
	}
	return out, dbRows.Err()
}

func rowsOrEmpty(rows []ExposureRow) []ExposureRow {
	if rows == nil {
		return []ExposureRow{}
	}
	return rows
}

func strField(row ExposureRow, key string) string {
	if v, ok := row[key].(string); ok {
		return v
	}
	return ""
}

func anyToFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
			return f
		}
	}
	if b, err := json.Marshal(v); err == nil {
		var f float64
		if err2 := json.Unmarshal(b, &f); err2 == nil {
			return f
		}
	}
	return 0
}

func normalizeExposureVal(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case pgtype.Numeric:
		if !t.Valid {
			return nil
		}
		f, _ := t.Float64Value()
		if f.Valid {
			return f.Float64
		}
		return nil
	case pgtype.Timestamptz:
		if !t.Valid {
			return nil
		}
		return t.Time.UTC().Format(time.RFC3339)
	case pgtype.Timestamp:
		if !t.Valid {
			return nil
		}
		return t.Time.UTC().Format(time.RFC3339)
	case pgtype.Date:
		if !t.Valid {
			return nil
		}
		return t.Time.UTC().Format("2006-01-02")
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	case []byte:
		return string(t)
	default:
		return v
	}
}

// CorrelationID builds a stable correlation key for FX notifications.
func CorrelationID(prefix, key string) string {
	return fmt.Sprintf("%s/%s/%d", prefix, key, time.Now().UnixMilli())
}

// FetchExposureIDsByBatch returns exposure header IDs linked to a batch.
func FetchExposureIDsByBatch(ctx context.Context, pool *pgxpool.Pool, batchID string) []string {
	if pool == nil || strings.TrimSpace(batchID) == "" {
		return nil
	}
	rows, err := pool.Query(ctx, `
		SELECT exposure_header_id::text
		FROM public.exposure_headers
		WHERE batch_id = $1::uuid
		  AND COALESCE(is_deleted, false) = false
	`, batchID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			ids = append(ids, id)
		}
	}
	return ids
}
