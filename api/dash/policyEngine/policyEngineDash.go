// Package policyEngine exposes read-only analytics endpoints for the
// policyengine_svc schema (execution_run / execution_log / policy_master).
//
// Endpoints registered in api/dash/routes.go:
//
//	POST /dash/policy-engine/overview            → all 7 charts + KPI in one fanned-out round trip (primary)
//	POST /dash/policy-engine/kpi                 → aggregate counts + breach rate + avg latency
//	POST /dash/policy-engine/result-trend        → daily pass/breach/error volume
//	POST /dash/policy-engine/module-breakdown    → breach/pass/error counts per module+sub-module
//	POST /dash/policy-engine/top-breaching       → top breaching policies (by breach count)
//	POST /dash/policy-engine/criticality-breakdown → breach counts by policy criticality/action
//	POST /dash/policy-engine/module-heatmap      → module x result matrix
//	POST /dash/policy-engine/latency-distribution → total_duration_ms histogram buckets
//	POST /dash/policy-engine/logs                → raw execution_log rows (all matches; filters only)
//
// execution_run carries one row per policy-check request with pre-aggregated
// pass/breach/error/duration counts (added 2026-07-30, see
// database/2026-07-30/executionRunExplainability.sql) — KPI/trend/breakdown/
// heatmap/latency all read from it, already indexed on
// (module_code, sub_module, started_at) and (entity_code, started_at). Only
// the two policy-level endpoints (top-breaching, criticality-breakdown) and
// the drill-down log view need execution_log, joined to policy_master.
//
// Each chart has its own fetch<X>/Get<X> pair (same split as notifDash.go)
// so GetOverview can fan every query out concurrently in one request — this
// is the primary dashboard call; the per-chart Get<X> endpoints exist for
// refetching a single chart after a chart-local filter change.
package policyEngine

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, _ int, v interface{}) {
	b, err := json.Marshal(v)
	if err != nil {
		api.RespondEnvelopeError(w, http.StatusInternalServerError, err.Error(), "")
		return
	}
	var fields map[string]interface{}
	if err := json.Unmarshal(b, &fields); err != nil {
		api.RespondEnvelopeSuccess(w, "Success", v)
		return
	}
	delete(fields, "success")
	api.RespondEnvelopeSuccessCompat(w, "Success", fields)
}

func errResp(w http.ResponseWriter, code int, msg string) {
	api.RespondEnvelopeError(w, code, msg, "")
}

// filterRequest is the common filter body shared by every endpoint. All
// fields are optional — zero values mean "no filter applied". DateRange
// covers both the "as of date" (custom start) and "as on date" (custom end)
// asks via the same custom/start/end shape notifDash.go already uses.
type filterRequest struct {
	DateRange   string `json:"dateRange"` // "today" | "24h" | "7d" | "30d" | "custom"
	CustomStart string `json:"customStartDate,omitempty"`
	CustomEnd   string `json:"customEndDate,omitempty"`
	EntityCode  string `json:"entityCode,omitempty"`
	ModuleCode  string `json:"moduleCode,omitempty"`
	SubModule   string `json:"subModule,omitempty"`
	Result      string `json:"result,omitempty"` // PASS | BREACH | ERROR, drill-down only
}

func (f filterRequest) dateWindow() (start, end time.Time) {
	end = time.Now()
	switch f.DateRange {
	case "today":
		y, m, d := end.Date()
		start = time.Date(y, m, d, 0, 0, 0, 0, end.Location())
	case "7d":
		start = end.Add(-7 * 24 * time.Hour)
	case "30d":
		start = end.Add(-30 * 24 * time.Hour)
	case "custom":
		if f.CustomStart != "" {
			if t, err := time.Parse(time.RFC3339, f.CustomStart); err == nil {
				start = t
			} else if t, err := time.Parse(constants.DateFormat, f.CustomStart); err == nil {
				start = t
			}
		}
		if f.CustomEnd != "" {
			if t, err := time.Parse(time.RFC3339, f.CustomEnd); err == nil {
				end = t
			} else if t, err := time.Parse(constants.DateFormat, f.CustomEnd); err == nil {
				end = t
			}
		}
		if start.IsZero() {
			start = end.Add(-30 * 24 * time.Hour)
		}
	default: // "24h" and any unknown value
		start = end.Add(-24 * time.Hour)
	}
	return
}

func decodeFilter(r *http.Request) (filterRequest, error) {
	var f filterRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil && err.Error() != "EOF" {
			return f, err
		}
	}
	return f, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. KPI
// ─────────────────────────────────────────────────────────────────────────────

type KPIResponse struct {
	Success        bool    `json:"success"`
	TotalChecks    int64   `json:"total_checks"`
	TotalEvaluated int64   `json:"total_evaluated"`
	TotalPass      int64   `json:"total_pass"`
	TotalBreach    int64   `json:"total_breach"`
	TotalError     int64   `json:"total_error"`
	BreachRatePct  float64 `json:"breach_rate_pct"`
	AvgDurationMs  float64 `json:"avg_duration_ms"`
	P95DurationMs  float64 `json:"p95_duration_ms"`
	ActivePolicies int64   `json:"active_policies"`
}

func fetchKPI(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (KPIResponse, error) {
	start, end := f.dateWindow()
	var kpi KPIResponse
	kpi.Success = true

	q := `
SELECT
    COUNT(*)                                                                      AS total_checks,
    COALESCE(SUM(evaluated_count), 0)                                             AS total_evaluated,
    COALESCE(SUM(pass_count), 0)                                                  AS total_pass,
    COALESCE(SUM(breach_count), 0)                                                AS total_breach,
    COALESCE(SUM(error_count), 0)                                                 AS total_error,
    COALESCE(AVG(total_duration_ms), 0)                                           AS avg_duration_ms,
    COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms), 0)  AS p95_duration_ms
FROM policyengine_svc.execution_run
WHERE started_at BETWEEN $1 AND $2
  AND ($3 = '' OR entity_code = $3)
  AND ($4 = '' OR module_code = $4)
  AND ($5 = '' OR sub_module  = $5)
`
	row := pool.QueryRow(ctx, q, start, end, f.EntityCode, f.ModuleCode, f.SubModule)
	if err := row.Scan(
		&kpi.TotalChecks, &kpi.TotalEvaluated, &kpi.TotalPass, &kpi.TotalBreach, &kpi.TotalError,
		&kpi.AvgDurationMs, &kpi.P95DurationMs,
	); err != nil {
		return kpi, err
	}
	if kpi.TotalEvaluated > 0 {
		kpi.BreachRatePct = float64(kpi.TotalBreach) / float64(kpi.TotalEvaluated) * 100
	}
	activeRow := pool.QueryRow(ctx, `
SELECT COUNT(*) FROM policyengine_svc.policy_master
WHERE COALESCE(is_deleted,false) = false AND status = 'Active' AND processing_status = 'APPROVED'`)
	_ = activeRow.Scan(&kpi.ActivePolicies)
	return kpi, nil
}

func GetKPI(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		v, err := fetchKPI(r.Context(), pool, f)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, v)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Result trend
// ─────────────────────────────────────────────────────────────────────────────

type TrendPoint struct {
	Bucket string `json:"bucket"`
	Pass   int64  `json:"pass"`
	Breach int64  `json:"breach"`
	Error  int64  `json:"error"`
}

type ResultTrendResponse struct {
	Success bool         `json:"success"`
	Points  []TrendPoint `json:"points"`
}

func fetchResultTrend(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (ResultTrendResponse, error) {
	start, end := f.dateWindow()
	resp := ResultTrendResponse{Success: true, Points: []TrendPoint{}}

	q := `
SELECT
    to_char(date_trunc('day', started_at), 'YYYY-MM-DD') AS bucket,
    COALESCE(SUM(pass_count), 0)   AS pass,
    COALESCE(SUM(breach_count), 0) AS breach,
    COALESCE(SUM(error_count), 0)  AS error
FROM policyengine_svc.execution_run
WHERE started_at BETWEEN $1 AND $2
  AND ($3 = '' OR entity_code = $3)
  AND ($4 = '' OR module_code = $4)
  AND ($5 = '' OR sub_module  = $5)
GROUP BY 1
ORDER BY 1
`
	rows, err := pool.Query(ctx, q, start, end, f.EntityCode, f.ModuleCode, f.SubModule)
	if err != nil {
		return resp, err
	}
	defer rows.Close()
	for rows.Next() {
		var p TrendPoint
		if err := rows.Scan(&p.Bucket, &p.Pass, &p.Breach, &p.Error); err != nil {
			return resp, err
		}
		resp.Points = append(resp.Points, p)
	}
	return resp, nil
}

func GetResultTrend(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		v, err := fetchResultTrend(r.Context(), pool, f)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, v)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Module breakdown
// ─────────────────────────────────────────────────────────────────────────────

type ModuleBreakdownRow struct {
	ModuleCode string `json:"module_code"`
	SubModule  string `json:"sub_module"`
	Pass       int64  `json:"pass"`
	Breach     int64  `json:"breach"`
	Error      int64  `json:"error"`
	TotalRuns  int64  `json:"total_runs"`
}

type ModuleBreakdownResponse struct {
	Success bool                 `json:"success"`
	Rows    []ModuleBreakdownRow `json:"rows"`
}

func fetchModuleBreakdown(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (ModuleBreakdownResponse, error) {
	start, end := f.dateWindow()
	resp := ModuleBreakdownResponse{Success: true, Rows: []ModuleBreakdownRow{}}

	q := `
SELECT
    COALESCE(module_code, ''),
    COALESCE(sub_module, ''),
    COALESCE(SUM(pass_count), 0)   AS pass,
    COALESCE(SUM(breach_count), 0) AS breach,
    COALESCE(SUM(error_count), 0)  AS error,
    COUNT(*)                       AS total_runs
FROM policyengine_svc.execution_run
WHERE started_at BETWEEN $1 AND $2
  AND ($3 = '' OR entity_code = $3)
  AND ($4 = '' OR module_code = $4)
  AND ($5 = '' OR sub_module  = $5)
GROUP BY module_code, sub_module
ORDER BY breach DESC, total_runs DESC
`
	rows, err := pool.Query(ctx, q, start, end, f.EntityCode, f.ModuleCode, f.SubModule)
	if err != nil {
		return resp, err
	}
	defer rows.Close()
	for rows.Next() {
		var m ModuleBreakdownRow
		if err := rows.Scan(&m.ModuleCode, &m.SubModule, &m.Pass, &m.Breach, &m.Error, &m.TotalRuns); err != nil {
			return resp, err
		}
		resp.Rows = append(resp.Rows, m)
	}
	return resp, nil
}

func GetModuleBreakdown(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		v, err := fetchModuleBreakdown(r.Context(), pool, f)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, v)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Top breaching policies
// ─────────────────────────────────────────────────────────────────────────────

type TopPolicyRow struct {
	PolicyCode  string `json:"policy_code"`
	PolicyName  string `json:"policy_name"`
	Criticality string `json:"criticality"`
	BreachCount int64  `json:"breach_count"`
}

type TopBreachingResponse struct {
	Success bool           `json:"success"`
	Rows    []TopPolicyRow `json:"rows"`
}

// Top N is an inherent property of "top breaching policies", not a pagination
// limit — matches how every other dashboard in this app treats a ranked list.
const topBreachingLimit = 15

func fetchTopBreaching(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (TopBreachingResponse, error) {
	start, end := f.dateWindow()
	resp := TopBreachingResponse{Success: true, Rows: []TopPolicyRow{}}

	q := `
SELECT
    COALESCE(l.policy_code, 'UNKNOWN') AS policy_code,
    COALESCE(p.name, l.policy_code, 'Unknown policy') AS policy_name,
    COALESCE(p.criticality, '') AS criticality,
    COUNT(*) AS breach_count
FROM policyengine_svc.execution_log l
LEFT JOIN policyengine_svc.policy_master p ON p.policy_id = l.policy_id
WHERE l.result = 'BREACH'
  AND l.evaluated_at BETWEEN $1 AND $2
  AND ($3 = '' OR l.entity_code = $3)
  AND ($4 = '' OR l.module_code = $4)
  AND ($5 = '' OR l.sub_module  = $5)
GROUP BY l.policy_code, p.name, p.criticality
ORDER BY breach_count DESC
LIMIT $6
`
	rows, err := pool.Query(ctx, q, start, end, f.EntityCode, f.ModuleCode, f.SubModule, topBreachingLimit)
	if err != nil {
		return resp, err
	}
	defer rows.Close()
	for rows.Next() {
		var tp TopPolicyRow
		if err := rows.Scan(&tp.PolicyCode, &tp.PolicyName, &tp.Criticality, &tp.BreachCount); err != nil {
			return resp, err
		}
		resp.Rows = append(resp.Rows, tp)
	}
	return resp, nil
}

func GetTopBreaching(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		v, err := fetchTopBreaching(r.Context(), pool, f)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, v)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Criticality breakdown
// ─────────────────────────────────────────────────────────────────────────────

type CriticalityRow struct {
	Criticality    string `json:"criticality"`
	ActionOnBreach string `json:"action_on_breach"`
	BreachCount    int64  `json:"breach_count"`
}

type CriticalityBreakdownResponse struct {
	Success bool             `json:"success"`
	Rows    []CriticalityRow `json:"rows"`
}

func fetchCriticalityBreakdown(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (CriticalityBreakdownResponse, error) {
	start, end := f.dateWindow()
	resp := CriticalityBreakdownResponse{Success: true, Rows: []CriticalityRow{}}

	q := `
SELECT
    COALESCE(p.criticality, 'Unknown') AS criticality,
    COALESCE(p.action_on_breach, 'Unknown') AS action_on_breach,
    COUNT(*) AS breach_count
FROM policyengine_svc.execution_log l
LEFT JOIN policyengine_svc.policy_master p ON p.policy_id = l.policy_id
WHERE l.result = 'BREACH'
  AND l.evaluated_at BETWEEN $1 AND $2
  AND ($3 = '' OR l.entity_code = $3)
  AND ($4 = '' OR l.module_code = $4)
  AND ($5 = '' OR l.sub_module  = $5)
GROUP BY p.criticality, p.action_on_breach
ORDER BY breach_count DESC
`
	rows, err := pool.Query(ctx, q, start, end, f.EntityCode, f.ModuleCode, f.SubModule)
	if err != nil {
		return resp, err
	}
	defer rows.Close()
	for rows.Next() {
		var c CriticalityRow
		if err := rows.Scan(&c.Criticality, &c.ActionOnBreach, &c.BreachCount); err != nil {
			return resp, err
		}
		resp.Rows = append(resp.Rows, c)
	}
	return resp, nil
}

func GetCriticalityBreakdown(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		v, err := fetchCriticalityBreakdown(r.Context(), pool, f)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, v)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Module x Result heatmap
// ─────────────────────────────────────────────────────────────────────────────

type HeatmapCell struct {
	ModuleCode string `json:"module_code"`
	Result     string `json:"result"`
	Count      int64  `json:"count"`
}

type ModuleHeatmapResponse struct {
	Success bool          `json:"success"`
	Cells   []HeatmapCell `json:"cells"`
}

func fetchModuleHeatmap(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (ModuleHeatmapResponse, error) {
	start, end := f.dateWindow()
	resp := ModuleHeatmapResponse{Success: true, Cells: []HeatmapCell{}}

	q := `
SELECT COALESCE(module_code, ''), 'PASS' AS result, COALESCE(SUM(pass_count),0)
FROM policyengine_svc.execution_run
WHERE started_at BETWEEN $1 AND $2
  AND ($3 = '' OR entity_code = $3) AND ($4 = '' OR module_code = $4) AND ($5 = '' OR sub_module = $5)
GROUP BY module_code
UNION ALL
SELECT COALESCE(module_code, ''), 'BREACH' AS result, COALESCE(SUM(breach_count),0)
FROM policyengine_svc.execution_run
WHERE started_at BETWEEN $1 AND $2
  AND ($3 = '' OR entity_code = $3) AND ($4 = '' OR module_code = $4) AND ($5 = '' OR sub_module = $5)
GROUP BY module_code
UNION ALL
SELECT COALESCE(module_code, ''), 'ERROR' AS result, COALESCE(SUM(error_count),0)
FROM policyengine_svc.execution_run
WHERE started_at BETWEEN $1 AND $2
  AND ($3 = '' OR entity_code = $3) AND ($4 = '' OR module_code = $4) AND ($5 = '' OR sub_module = $5)
GROUP BY module_code
`
	rows, err := pool.Query(ctx, q, start, end, f.EntityCode, f.ModuleCode, f.SubModule)
	if err != nil {
		return resp, err
	}
	defer rows.Close()
	for rows.Next() {
		var c HeatmapCell
		if err := rows.Scan(&c.ModuleCode, &c.Result, &c.Count); err != nil {
			return resp, err
		}
		resp.Cells = append(resp.Cells, c)
	}
	return resp, nil
}

func GetModuleHeatmap(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		v, err := fetchModuleHeatmap(r.Context(), pool, f)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, v)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Latency distribution
// ─────────────────────────────────────────────────────────────────────────────

type LatencyBucket struct {
	Bucket string `json:"bucket"`
	Count  int64  `json:"count"`
}

type LatencyDistributionResponse struct {
	Success bool            `json:"success"`
	Buckets []LatencyBucket `json:"buckets"`
}

var latencyBucketOrder = []string{"0-50ms", "50-100ms", "100-200ms", "200-300ms", "300-500ms", "500ms-1s", "1s+"}

func fetchLatencyDistribution(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (LatencyDistributionResponse, error) {
	start, end := f.dateWindow()

	q := `
SELECT
    CASE
        WHEN total_duration_ms < 50   THEN '0-50ms'
        WHEN total_duration_ms < 100  THEN '50-100ms'
        WHEN total_duration_ms < 200  THEN '100-200ms'
        WHEN total_duration_ms < 300  THEN '200-300ms'
        WHEN total_duration_ms < 500  THEN '300-500ms'
        WHEN total_duration_ms < 1000 THEN '500ms-1s'
        ELSE '1s+'
    END AS bucket,
    COUNT(*) AS count
FROM policyengine_svc.execution_run
WHERE started_at BETWEEN $1 AND $2
  AND ($3 = '' OR entity_code = $3)
  AND ($4 = '' OR module_code = $4)
  AND ($5 = '' OR sub_module  = $5)
GROUP BY 1
`
	rows, err := pool.Query(ctx, q, start, end, f.EntityCode, f.ModuleCode, f.SubModule)
	counts := make(map[string]int64, len(latencyBucketOrder))
	if err != nil {
		return LatencyDistributionResponse{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var b LatencyBucket
		if err := rows.Scan(&b.Bucket, &b.Count); err != nil {
			return LatencyDistributionResponse{}, err
		}
		counts[b.Bucket] = b.Count
	}
	buckets := make([]LatencyBucket, len(latencyBucketOrder))
	for i, k := range latencyBucketOrder {
		buckets[i] = LatencyBucket{Bucket: k, Count: counts[k]}
	}
	return LatencyDistributionResponse{Success: true, Buckets: buckets}, nil
}

func GetLatencyDistribution(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		v, err := fetchLatencyDistribution(r.Context(), pool, f)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, v)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Drill-down log rows
// ─────────────────────────────────────────────────────────────────────────────

type LogRow struct {
	ExecutionID   string  `json:"execution_id"`
	CorrelationID *string `json:"correlation_id,omitempty"`
	ModuleCode    *string `json:"module_code,omitempty"`
	SubModule     *string `json:"sub_module,omitempty"`
	EventCode     *string `json:"event_code,omitempty"`
	HandlerName   *string `json:"handler_name,omitempty"`
	ApiPath       *string `json:"api_path,omitempty"`
	ActorUserID   *string `json:"actor_user_id,omitempty"`
	EntityCode    *string `json:"entity_code,omitempty"`
	PolicyCode    *string `json:"policy_code,omitempty"`
	PolicyName    *string `json:"policy_name,omitempty"`
	Result        string  `json:"result"`
	ActionFired   *string `json:"action_fired,omitempty"`
	DetailMessage *string `json:"detail_message,omitempty"`
	FailReason    *string `json:"fail_reason,omitempty"`
	ComparedVar   *string `json:"compared_variable,omitempty"`
	ComparedValue *string `json:"compared_value,omitempty"`
	LimitValue    *string `json:"limit_value,omitempty"`
	DurationMs    *int    `json:"duration_ms,omitempty"`
	EvaluatedAt   string  `json:"evaluated_at"`
}

type LogsResponse struct {
	Success    bool     `json:"success"`
	Rows       []LogRow `json:"rows"`
	TotalCount int64    `json:"total_count"`
}

// fetchLogs returns every matching row — no pagination, no limit, matching
// the convention notifDash.go already established for this app's list
// endpoints. Used for the drill-down click-through, not the primary overview.
func fetchLogs(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (LogsResponse, error) {
	start, end := f.dateWindow()
	resp := LogsResponse{Success: true, Rows: []LogRow{}}

	q := `
SELECT
    l.execution_id, l.correlation_id, l.module_code, l.sub_module, l.event_code,
    l.handler_name, l.api_path, l.actor_user_id, l.entity_code,
    l.policy_code, p.name,
    l.result, l.action_fired, l.detail_message, l.fail_reason,
    l.compared_variable, l.compared_value, l.limit_value,
    l.duration_ms, l.evaluated_at
FROM policyengine_svc.execution_log l
LEFT JOIN policyengine_svc.policy_master p ON p.policy_id = l.policy_id
WHERE l.evaluated_at BETWEEN $1 AND $2
  AND ($3 = '' OR l.entity_code = $3)
  AND ($4 = '' OR l.module_code = $4)
  AND ($5 = '' OR l.sub_module  = $5)
  AND ($6 = '' OR l.result      = $6)
ORDER BY l.evaluated_at DESC
`
	rows, err := pool.Query(ctx, q, start, end, f.EntityCode, f.ModuleCode, f.SubModule, f.Result)
	if err != nil {
		return resp, err
	}
	defer rows.Close()
	for rows.Next() {
		var l LogRow
		var evaluatedAt time.Time
		if err := rows.Scan(
			&l.ExecutionID, &l.CorrelationID, &l.ModuleCode, &l.SubModule, &l.EventCode,
			&l.HandlerName, &l.ApiPath, &l.ActorUserID, &l.EntityCode,
			&l.PolicyCode, &l.PolicyName,
			&l.Result, &l.ActionFired, &l.DetailMessage, &l.FailReason,
			&l.ComparedVar, &l.ComparedValue, &l.LimitValue,
			&l.DurationMs, &evaluatedAt,
		); err != nil {
			return resp, err
		}
		l.EvaluatedAt = evaluatedAt.Format(time.RFC3339)
		resp.Rows = append(resp.Rows, l)
	}
	resp.TotalCount = int64(len(resp.Rows))
	return resp, nil
}

func GetLogs(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		v, err := fetchLogs(r.Context(), pool, f)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, v)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. Overview — fans every chart query out concurrently, one round trip
// ─────────────────────────────────────────────────────────────────────────────

type OverviewResponse struct {
	Success      bool                         `json:"success"`
	KPI          KPIResponse                  `json:"kpi"`
	ResultTrend  ResultTrendResponse          `json:"result_trend"`
	ModuleBreak  ModuleBreakdownResponse      `json:"module_breakdown"`
	TopBreaching TopBreachingResponse         `json:"top_breaching"`
	Criticality  CriticalityBreakdownResponse `json:"criticality_breakdown"`
	Heatmap      ModuleHeatmapResponse        `json:"module_heatmap"`
	Latency      LatencyDistributionResponse  `json:"latency_distribution"`
}

func GetOverview(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		ctx := r.Context()

		type kpiRes struct {
			v KPIResponse
			e error
		}
		type trendRes struct {
			v ResultTrendResponse
			e error
		}
		type breakRes struct {
			v ModuleBreakdownResponse
			e error
		}
		type topRes struct {
			v TopBreachingResponse
			e error
		}
		type critRes struct {
			v CriticalityBreakdownResponse
			e error
		}
		type heatRes struct {
			v ModuleHeatmapResponse
			e error
		}
		type latRes struct {
			v LatencyDistributionResponse
			e error
		}

		kpiC := make(chan kpiRes, 1)
		trendC := make(chan trendRes, 1)
		breakC := make(chan breakRes, 1)
		topC := make(chan topRes, 1)
		critC := make(chan critRes, 1)
		heatC := make(chan heatRes, 1)
		latC := make(chan latRes, 1)

		go func() { v, e := fetchKPI(ctx, pool, f); kpiC <- kpiRes{v, e} }()
		go func() { v, e := fetchResultTrend(ctx, pool, f); trendC <- trendRes{v, e} }()
		go func() { v, e := fetchModuleBreakdown(ctx, pool, f); breakC <- breakRes{v, e} }()
		go func() { v, e := fetchTopBreaching(ctx, pool, f); topC <- topRes{v, e} }()
		go func() { v, e := fetchCriticalityBreakdown(ctx, pool, f); critC <- critRes{v, e} }()
		go func() { v, e := fetchModuleHeatmap(ctx, pool, f); heatC <- heatRes{v, e} }()
		go func() { v, e := fetchLatencyDistribution(ctx, pool, f); latC <- latRes{v, e} }()

		r0 := <-kpiC
		r1 := <-trendC
		r2 := <-breakC
		r3 := <-topC
		r4 := <-critC
		r5 := <-heatC
		r6 := <-latC

		for _, e := range []error{r0.e, r1.e, r2.e, r3.e, r4.e, r5.e, r6.e} {
			if e != nil {
				errResp(w, http.StatusInternalServerError, e.Error())
				return
			}
		}

		writeJSON(w, http.StatusOK, OverviewResponse{
			Success:      true,
			KPI:          r0.v,
			ResultTrend:  r1.v,
			ModuleBreak:  r2.v,
			TopBreaching: r3.v,
			Criticality:  r4.v,
			Heatmap:      r5.v,
			Latency:      r6.v,
		})
	}
}
