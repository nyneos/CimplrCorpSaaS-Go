package varianceengine

// varianceengine — pluggable variance & exception engine.
//
// Usage (any module):
//   items := varianceengine.Compare("FD_CLOSURE", closureID, entityID, runID, rules)
//   varianceengine.PersistVariances(ctx, pool, items)
//   varianceengine.UpdateRecordFlags(ctx, pool, "investment.fd_closure_request", "closure_request_id", closureID, runID, items)

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type QueryExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// ─── Constants ────────────────────────────────────────────────────────────────

const (
	TypeAmount   = "AMOUNT"
	TypeDate     = "DATE"
	TypeDays     = "DAYS"
	TypeRate     = "RATE"
	TypeIdentity = "IDENTITY"
	TypeOther    = "OTHER"

	StatusOpen      = "OPEN"
	StatusResolved  = "RESOLVED"
	StatusException = "EXCEPTION"

	PriorityHigh   = "HIGH"
	PriorityMedium = "MEDIUM"
	PriorityLow    = "LOW"
)

// ─── Types ────────────────────────────────────────────────────────────────────

// Rule describes one field comparison the calling module wants to validate.
type Rule struct {
	FieldName     string
	VarianceType  string  // TypeAmount | TypeDate | TypeDays | TypeRate | TypeIdentity | TypeOther
	ExpectedValue string  // what the system/DB holds (source of truth)
	ActualValue   string  // what the user submitted / incoming payload
	Priority      string  // PriorityHigh | PriorityMedium | PriorityLow — defaults to MEDIUM
	Tolerance     float64 // for numeric/date: raise variance only when |delta| > Tolerance
	SystemComment string  // optional hint written into system_comment
}

// VarianceItem is one evaluated rule — whether clean or flagged.
type VarianceItem struct {
	VarianceID    string // populated after PersistVariances
	ModuleCode    string
	RecordID      string
	EntityID      string
	RunID         string
	FieldName     string
	VarianceType  string
	ExpectedValue string
	ActualValue   string
	VarianceDelta float64
	Priority      string
	Status        string // OPEN initially
	IsException   bool
	SystemComment string
	HasVariance   bool // false = values match within tolerance
}

// ResolveRequest marks a variance row RESOLVED or EXCEPTION.
type ResolveRequest struct {
	VarianceID      string
	Resolution      string // RESOLVED | EXCEPTION
	Reason          string
	ResolvedBy      string
	ResolvedByEmail string
	Evidence        string
}

// ─── Core: Compare ────────────────────────────────────────────────────────────

// Compare evaluates all rules and returns one VarianceItem per rule.
// Items where HasVariance=false are clean fields — still returned for full response enrichment.
func Compare(moduleCode, recordID, entityID, runID string, rules []Rule) []VarianceItem {
	items := make([]VarianceItem, 0, len(rules))
	for _, r := range rules {
		if r.Priority == "" {
			r.Priority = PriorityMedium
		}
		item := VarianceItem{
			ModuleCode:    moduleCode,
			RecordID:      recordID,
			EntityID:      entityID,
			RunID:         runID,
			FieldName:     r.FieldName,
			VarianceType:  r.VarianceType,
			ExpectedValue: r.ExpectedValue,
			ActualValue:   r.ActualValue,
			Priority:      r.Priority,
			Status:        StatusOpen,
			SystemComment: r.SystemComment,
		}

		switch r.VarianceType {
		case TypeAmount, TypeDays, TypeRate:
			exp := parseFloat(r.ExpectedValue)
			act := parseFloat(r.ActualValue)
			delta := act - exp
			item.VarianceDelta = roundFour(delta)
			if math.Abs(delta) > r.Tolerance {
				item.HasVariance = true
				if item.SystemComment == "" {
					item.SystemComment = fmt.Sprintf("Expected %.4f, got %.4f (delta %.4f)", exp, act, delta)
				}
			}
		case TypeDate:
			expT := parseDate(r.ExpectedValue)
			actT := parseDate(r.ActualValue)
			if !expT.IsZero() && !actT.IsZero() {
				diffDays := actT.Sub(expT).Hours() / 24
				item.VarianceDelta = roundFour(diffDays)
				if math.Abs(diffDays) > r.Tolerance {
					item.HasVariance = true
					if item.SystemComment == "" {
						item.SystemComment = fmt.Sprintf("Expected %s, got %s (%+.0f days)",
							r.ExpectedValue, r.ActualValue, diffDays)
					}
				}
			} else if !strings.EqualFold(strings.TrimSpace(r.ExpectedValue), strings.TrimSpace(r.ActualValue)) {
				item.HasVariance = true
				if item.SystemComment == "" {
					item.SystemComment = fmt.Sprintf("Expected %s, got %s", r.ExpectedValue, r.ActualValue)
				}
			}
		default: // TypeIdentity, TypeOther
			if !strings.EqualFold(strings.TrimSpace(r.ExpectedValue), strings.TrimSpace(r.ActualValue)) {
				item.HasVariance = true
				if item.SystemComment == "" {
					item.SystemComment = fmt.Sprintf("Expected %q, got %q", r.ExpectedValue, r.ActualValue)
				}
			}
		}
		items = append(items, item)
	}
	return items
}

// ─── DB: PersistVariances ─────────────────────────────────────────────────────

// PersistVariances upserts variance rows into public.variance_log.
// Only items where HasVariance=true are written.
// If an OPEN row already exists for (record_id, field_name) it is updated rather than duplicated.
func PersistVariances(ctx context.Context, pool QueryExecutor, items []VarianceItem) error {
	for i, item := range items {
		if !item.HasVariance {
			continue
		}
		var existingID string
		_ = pool.QueryRow(ctx,
			`SELECT variance_id FROM public.variance_log
			 WHERE record_id=$1 AND field_name=$2 AND status='OPEN' LIMIT 1`,
			item.RecordID, item.FieldName).Scan(&existingID)

		if existingID != "" {
			_, err := pool.Exec(ctx, `
				UPDATE public.variance_log SET
				  run_id=$1, expected_value=$2, actual_value=$3,
				  variance_delta=$4, system_comment=$5, updated_at=NOW()
				WHERE variance_id=$6`,
				item.RunID, item.ExpectedValue, item.ActualValue,
				item.VarianceDelta, item.SystemComment, existingID)
			if err != nil {
				return fmt.Errorf("PersistVariances update %s: %w", item.FieldName, err)
			}
			items[i].VarianceID = existingID
		} else {
			var newID string
			err := pool.QueryRow(ctx, `
				INSERT INTO public.variance_log
				  (module_code, record_id, entity_id, run_id, field_name, variance_type,
				   expected_value, actual_value, variance_delta, priority,
				   status, is_exception, system_comment)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'OPEN',false,$11)
				RETURNING variance_id`,
				item.ModuleCode, item.RecordID, item.EntityID, item.RunID,
				item.FieldName, item.VarianceType,
				item.ExpectedValue, item.ActualValue, item.VarianceDelta,
				item.Priority, item.SystemComment).Scan(&newID)
			if err != nil {
				return fmt.Errorf("PersistVariances insert %s: %w", item.FieldName, err)
			}
			items[i].VarianceID = newID
		}
	}
	return nil
}

// ─── DB: UpdateRecordFlags ────────────────────────────────────────────────────

// UpdateRecordFlags stamps has_variance, has_unresolved_variance, last_validated_at,
// last_variance_run_id on the calling module's record.
// table: "investment.fd_closure_request", pkCol: "closure_request_id"
func UpdateRecordFlags(ctx context.Context, pool QueryExecutor, table, pkCol, pkVal, runID string, items []VarianceItem) error {
	hasAny := false
	hasUnresolved := false
	for _, item := range items {
		if item.HasVariance {
			hasAny = true
			if item.Status == StatusOpen {
				hasUnresolved = true
			}
		}
	}
	sql := fmt.Sprintf(
		`UPDATE %s SET has_variance=$1, has_unresolved_variance=$2, last_validated_at=NOW(), last_variance_run_id=$3 WHERE %s=$4`,
		table, pkCol)
	_, err := pool.Exec(ctx, sql, hasAny, hasUnresolved, runID, pkVal)
	return err
}

// ─── DB: AutoResolveCleared ───────────────────────────────────────────────────

// AutoResolveCleared scans items for fields that are now clean (HasVariance=false).
// For each clean field it finds the existing OPEN variance row (if any) and marks it RESOLVED.
// This is the core of the "re-validate = resolve" workflow:
//
//	the user fixes a value → re-calls /validate → engine produces HasVariance=false for that field
//	→ AutoResolveCleared fires → OPEN row becomes RESOLVED automatically.
func AutoResolveCleared(ctx context.Context, pool QueryExecutor, recordID string, items []VarianceItem, resolvedBy, resolvedByEmail string) error {
	for _, item := range items {
		if item.HasVariance {
			continue // still has a difference — leave OPEN
		}
		// Field is now clean — look for an OPEN row to resolve
		var existingID string
		_ = pool.QueryRow(ctx,
			`SELECT variance_id FROM public.variance_log
			 WHERE record_id=$1 AND field_name=$2 AND status='OPEN' LIMIT 1`,
			recordID, item.FieldName).Scan(&existingID)
		if existingID == "" {
			continue // nothing to resolve
		}
		_, err := pool.Exec(ctx, `
			UPDATE public.variance_log SET
			  status='RESOLVED',
			  resolution_reason='Auto-resolved on re-validation — values now match',
			  resolved_by=$1, resolved_by_email=$2,
			  resolved_at=NOW(), updated_at=NOW()
			WHERE variance_id=$3`,
			resolvedBy, resolvedByEmail, existingID)
		if err != nil {
			return fmt.Errorf("AutoResolveCleared %s: %w", item.FieldName, err)
		}
	}
	return nil
}

// ─── DB: RefreshUnresolvedFlag ────────────────────────────────────────────────

// RefreshUnresolvedFlag re-queries variance_log after a resolve/exception action
// and updates has_unresolved_variance on the parent record.
func RefreshUnresolvedFlag(ctx context.Context, pool QueryExecutor, table, pkCol, recordID string) error {
	var openCount int
	_ = pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM public.variance_log WHERE record_id=$1 AND status='OPEN'`,
		recordID).Scan(&openCount)
	sql := fmt.Sprintf(`UPDATE %s SET has_unresolved_variance=$1, updated_at=NOW() WHERE %s=$2`, table, pkCol)
	_, err := pool.Exec(ctx, sql, openCount > 0, recordID)
	return err
}

// ─── DB: ResolveVariance ─────────────────────────────────────────────────────

// ResolveVariance marks one variance_log row RESOLVED or EXCEPTION.
func ResolveVariance(ctx context.Context, pool QueryExecutor, req ResolveRequest) error {
	if req.Resolution != StatusResolved && req.Resolution != StatusException {
		return fmt.Errorf("invalid resolution %q — must be RESOLVED or EXCEPTION", req.Resolution)
	}
	_, err := pool.Exec(ctx, `
		UPDATE public.variance_log SET
		  status=$1, is_exception=$2, resolution_reason=$3,
		  resolved_by=$4, resolved_by_email=$5,
		  evidence=$6, resolved_at=NOW(), updated_at=NOW()
		WHERE variance_id=$7`,
		req.Resolution, req.Resolution == StatusException,
		req.Reason, req.ResolvedBy, req.ResolvedByEmail,
		req.Evidence, req.VarianceID)
	return err
}

// ─── DB: GetVariances ────────────────────────────────────────────────────────

// GetVariances returns all variance_log rows for a record_id, newest first.
func GetVariances(ctx context.Context, pool QueryExecutor, recordID string) ([]map[string]interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT variance_id, module_code, record_id, entity_id, run_id,
		       field_name, variance_type, expected_value, actual_value,
		       variance_delta, priority, status, is_exception,
		       system_comment, resolution_reason, resolved_by_email,
		       resolved_at, evidence, created_at, updated_at
		FROM public.variance_log
		WHERE record_id=$1
		ORDER BY created_at DESC`, recordID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]interface{}
	for rows.Next() {
		vals, _ := rows.Values()
		cols := rows.FieldDescriptions()
		row := make(map[string]interface{}, len(cols))
		for i, c := range cols {
			row[string(c.Name)] = vals[i]
		}
		out = append(out, row)
	}
	return out, nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return v
}

func parseDate(s string) time.Time {
	for _, layout := range []string{
		constants.DateFormat,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05Z",
	} {
		if t, err := time.Parse(layout, strings.TrimSpace(s)); err == nil {
			return t
		}
	}
	return time.Time{}
}

func roundFour(v float64) float64 {
	return math.Round(v*10000) / 10000
}

// NewRunID generates a short run identifier for grouping one validate call.
func NewRunID() string {
	return fmt.Sprintf("VRN-%s", strings.ToUpper(
		fmt.Sprintf("%x", time.Now().UnixNano())[:7],
	))
}
