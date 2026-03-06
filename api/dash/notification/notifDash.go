// Package notification exposes read-only dashboard endpoints for the
// notification_svc schema.  Every endpoint lives under /dash/notification/
// and reads from notification_svc.outbox, notification_svc.send_history
// and notification_svc.notification_config.
//
// Endpoints registered in dash.go:
//
//	POST /dash/notification/kpi               → aggregate counts + delivery rates
//	POST /dash/notification/logs              → paginated outbox log rows
//	POST /dash/notification/channel-breakdown → sent/failed counts per channel
//	POST /dash/notification/hourly-trend      → hourly delivery volume
//	POST /dash/notification/top-events        → most-fired events with success rate
//	POST /dash/notification/timeline          → full timeline for one correlation_id
//	POST /dash/notification/retry-stats       → retry-depth distribution + dead-letter
//	POST /dash/notification/send-history      → provider-level send attempts
package notification

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func errResp(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]interface{}{"success": false, "error": msg})
}

// filterRequest is the common filter body shared by all endpoints.
// All fields are optional — zero values mean "no filter applied".
type filterRequest struct {
	DateRange     string `json:"dateRange"` // "today" | "24h" | "7d" | "custom"
	CustomStart   string `json:"customStartDate,omitempty"`
	CustomEnd     string `json:"customEndDate,omitempty"`
	Channel       string `json:"channel,omitempty"`
	Status        string `json:"status,omitempty"`
	EventType     string `json:"eventType,omitempty"`
	Provider      string `json:"provider,omitempty"`
	CorrelationID string `json:"correlationId,omitempty"`
	Recipient     string `json:"recipient,omitempty"`
	MinRetry      int    `json:"minRetryCount"`
	MaxRetry      int    `json:"maxRetryCount"`
	Page          int    `json:"page"`
	Limit         int    `json:"limit"`
	HourWindow    int    `json:"hourWindow"` // for hourly-trend; default 24
	LogInfo       bool   `json:"logInfo"`    // when true: include variables_payload, template, retry_history in log rows
}

func (f filterRequest) dateWindow() (start, end time.Time) {
	end = time.Now()
	switch f.DateRange {
	case "today":
		y, m, d := end.Date()
		start = time.Date(y, m, d, 0, 0, 0, 0, end.Location())
	case "7d":
		start = end.Add(-7 * 24 * time.Hour)
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
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Page <= 0 {
		f.Page = 1
	}
	if f.MaxRetry == 0 {
		f.MaxRetry = 999
	}
	if f.HourWindow <= 0 {
		f.HourWindow = 24
	}
	return f, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. KPI  —  POST /dash/notification/kpi
// ─────────────────────────────────────────────────────────────────────────────

type KPIResponse struct {
	Success         bool    `json:"success"`
	TotalDispatched int64   `json:"total_dispatched"`
	TotalSent       int64   `json:"total_sent"`
	TotalFailed     int64   `json:"total_failed"`
	TotalDead       int64   `json:"total_dead"`
	TotalPending    int64   `json:"total_pending"`
	TotalRetrying   int64   `json:"total_retrying"`
	DeliveryRate    float64 `json:"delivery_rate_pct"`
	FailureRate     float64 `json:"failure_rate_pct"`
	AvgRetryCount   float64 `json:"avg_retry_count"`
	AvgLatencyMs    float64 `json:"avg_latency_ms"`
	ChannelEmail    int64   `json:"channel_email"`
	ChannelSMS      int64   `json:"channel_sms"`
	ChannelPush     int64   `json:"channel_push"`
	ChannelWhatsApp int64   `json:"channel_whatsapp"`
}

func GetKPI(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		start, end := f.dateWindow()

		q := `
SELECT
    COUNT(*)                                                                        AS total_dispatched,
    COUNT(*) FILTER (WHERE processing_status = 'SENT')                             AS total_sent,
    COUNT(*) FILTER (WHERE processing_status = 'FAILED')                           AS total_failed,
    COUNT(*) FILTER (WHERE processing_status = 'DEAD')                             AS total_dead,
    COUNT(*) FILTER (WHERE processing_status IN ('PENDING','PROCESSING'))           AS total_pending,
    COUNT(*) FILTER (WHERE retry_count > 0
                       AND processing_status NOT IN ('SENT','DEAD'))               AS total_retrying,
    COUNT(*) FILTER (WHERE channel = 'EMAIL')                                      AS ch_email,
    COUNT(*) FILTER (WHERE channel = 'SMS')                                        AS ch_sms,
    COUNT(*) FILTER (WHERE channel = 'PUSH')                                       AS ch_push,
    COUNT(*) FILTER (WHERE channel = 'WHATSAPP')                                   AS ch_wa,
    COALESCE(AVG(retry_count), 0)                                                  AS avg_retry,
    COALESCE(
        AVG(EXTRACT(EPOCH FROM (sent_at - scheduled_at)) * 1000)
            FILTER (WHERE sent_at IS NOT NULL), 0)                                 AS avg_latency_ms
FROM notification_svc.outbox
WHERE created_at BETWEEN $1 AND $2
  AND ($3 = '' OR channel            = $3)
  AND ($4 = '' OR processing_status  = $4)
  AND ($5 = '' OR correlation_id ILIKE '%' || $5 || '%')
  AND ($6 = '' OR recipient_email ILIKE '%' || $6 || '%'
               OR recipient_phone ILIKE '%' || $6 || '%')
  AND retry_count BETWEEN $7 AND $8
`
		row := pool.QueryRow(context.Background(), q,
			start, end,
			f.Channel, f.Status, f.CorrelationID, f.Recipient,
			f.MinRetry, f.MaxRetry,
		)
		var kpi KPIResponse
		kpi.Success = true
		if err := row.Scan(
			&kpi.TotalDispatched, &kpi.TotalSent, &kpi.TotalFailed, &kpi.TotalDead,
			&kpi.TotalPending, &kpi.TotalRetrying,
			&kpi.ChannelEmail, &kpi.ChannelSMS, &kpi.ChannelPush, &kpi.ChannelWhatsApp,
			&kpi.AvgRetryCount, &kpi.AvgLatencyMs,
		); err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		if kpi.TotalDispatched > 0 {
			kpi.DeliveryRate = float64(kpi.TotalSent) / float64(kpi.TotalDispatched) * 100
			kpi.FailureRate = float64(kpi.TotalFailed+kpi.TotalDead) / float64(kpi.TotalDispatched) * 100
		}
		writeJSON(w, http.StatusOK, kpi)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Log rows  —  POST /dash/notification/logs
// ─────────────────────────────────────────────────────────────────────────────

// RetryAttempt represents a single delivery attempt from send_history,
// shown in the "Retry History" panel of a log row detail view.
type RetryAttempt struct {
	AttemptNumber    int     `json:"attempt_number"`
	Status           string  `json:"status"`
	AttemptedAt      string  `json:"attempted_at"`
	ProviderResponse *string `json:"provider_response,omitempty"`
	ProviderMsgID    *string `json:"provider_message_id,omitempty"`
}

// LogRowTemplate carries the template metadata shown in the
// "Technical Details" panel for a specific log row.
type LogRowTemplate struct {
	AuditID      string `json:"audit_id"`
	VersionLabel string `json:"version_label"`
	Subject      string `json:"subject"`
	BodyText     string `json:"body_text"`
	IsHTML       bool   `json:"is_html"`
}

type LogRow struct {
	OutboxID        string           `json:"outbox_id"`
	CorrelationID   string           `json:"correlation_id"`
	EventName       string           `json:"event_name"`
	Channel         string           `json:"channel"`
	Recipient       string           `json:"recipient"`
	Status          string           `json:"status"`
	RetryCount      int              `json:"retry_count"`
	LatencyMs       *float64         `json:"latency_ms,omitempty"`
	CreatedAt       string           `json:"created_at"`
	UpdatedAt       *string          `json:"updated_at,omitempty"`
	ErrorMessage    *string          `json:"error_message,omitempty"`
	AuditID         string           `json:"audit_id"`
	SenderName      *string          `json:"sender_name,omitempty"`
	SenderEmail     *string          `json:"sender_email,omitempty"`
	RenderedSubject *string          `json:"rendered_subject,omitempty"`
	PriorityLevel   int              `json:"priority_level"`
	// Technical Details panel
	VariablesPayload map[string]interface{} `json:"variables_payload"`
	// Template details panel
	Template *LogRowTemplate `json:"template,omitempty"`
	// Retry History panel
	RetryHistory []RetryAttempt `json:"retry_history"`
}

type LogsResponse struct {
	Success    bool     `json:"success"`
	Rows       []LogRow `json:"rows"`
	TotalCount int64    `json:"total_count"`
	Page       int      `json:"page"`
	Limit      int      `json:"limit"`
	TotalPages int64    `json:"total_pages"`
}

func GetLogs(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		start, end := f.dateWindow()
		offset := (f.Page - 1) * f.Limit

		// Choose variables_payload column based on log_info flag.
		varCol := "'{}'"
		if f.LogInfo {
			varCol = "COALESCE(o.variables_payload::text, '{}')"
		}

		baseWhere := `
FROM notification_svc.outbox o
JOIN notification_svc.event e ON e.event_id = o.event_id
WHERE o.created_at BETWEEN $1 AND $2
  AND ($3 = '' OR o.channel = $3)
  AND ($4 = '' OR o.processing_status = $4)
  AND ($5 = '' OR o.event_id = $5 OR e.event_display_name ILIKE '%' || $5 || '%')
  AND ($6 = '' OR o.correlation_id ILIKE '%' || $6 || '%')
  AND ($7 = '' OR o.recipient_email ILIKE '%' || $7 || '%'
               OR o.recipient_phone ILIKE '%' || $7 || '%')
  AND o.retry_count BETWEEN $8 AND $9`

		var total int64
		if err := pool.QueryRow(r.Context(), `SELECT COUNT(*) `+baseWhere,
			start, end, f.Channel, f.Status, f.EventType, f.CorrelationID, f.Recipient,
			f.MinRetry, f.MaxRetry,
		).Scan(&total); err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}

		rowQ := `
SELECT
    o.outbox_id, o.correlation_id,
    COALESCE(e.event_display_name, o.event_id)                                        AS event_name,
    o.channel,
    COALESCE(o.recipient_email, o.recipient_phone, o.recipient_user_id, '')           AS recipient,
    o.processing_status, o.retry_count,
    CASE WHEN o.sent_at IS NOT NULL
         THEN EXTRACT(EPOCH FROM (o.sent_at - o.scheduled_at)) * 1000
         ELSE NULL END                                                                AS latency_ms,
    o.created_at, o.processed_at, o.last_error, o.audit_id::text,
    o.sender_name, o.sender_email, o.rendered_subject, o.priority_level,
    ` + varCol + ` ` + baseWhere + `
ORDER BY o.created_at DESC
LIMIT $10 OFFSET $11`

		rows, err := pool.Query(r.Context(), rowQ,
			start, end, f.Channel, f.Status, f.EventType, f.CorrelationID, f.Recipient,
			f.MinRetry, f.MaxRetry, f.Limit, offset,
		)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		result := make([]LogRow, 0, f.Limit)
		for rows.Next() {
			var row LogRow
			var createdAt time.Time
			var processedAt *time.Time
			var latencyMs *float64
			var variablesRaw string
			if err := rows.Scan(
				&row.OutboxID, &row.CorrelationID, &row.EventName,
				&row.Channel, &row.Recipient, &row.Status,
				&row.RetryCount, &latencyMs,
				&createdAt, &processedAt,
				&row.ErrorMessage,
				&row.AuditID, &row.SenderName, &row.SenderEmail,
				&row.RenderedSubject, &row.PriorityLevel,
				&variablesRaw,
			); err != nil {
				continue
			}
			row.LatencyMs = latencyMs
			row.CreatedAt = createdAt.UTC().Format(time.RFC3339)
			if processedAt != nil {
				s := processedAt.UTC().Format(time.RFC3339)
				row.UpdatedAt = &s
			}
			if f.LogInfo {
				row.VariablesPayload = make(map[string]interface{})
				_ = json.Unmarshal([]byte(variablesRaw), &row.VariablesPayload)
			} else {
				row.VariablesPayload = map[string]interface{}{}
			}
			row.RetryHistory = []RetryAttempt{}
			result = append(result, row)
		}
		rows.Close() // release connection back to pool before batch queries

		// ── Batch enrich (log_info=true only) ──────────────────────────────────
		if f.LogInfo && len(result) > 0 {
			outboxIDs := make([]string, len(result))
			auditIDs := make([]string, 0, len(result))
			auditIDSet := make(map[string]bool, len(result))
			for i, row := range result {
				outboxIDs[i] = row.OutboxID
				if row.AuditID != "" && !auditIDSet[row.AuditID] {
					auditIDs = append(auditIDs, row.AuditID)
					auditIDSet[row.AuditID] = true
				}
			}

			type tplRes struct {
				m map[string]LogRowTemplate
				e error
			}
			type histRes struct {
				m map[string][]RetryAttempt
				e error
			}
			tplC := make(chan tplRes, 1)
			histC := make(chan histRes, 1)

			go func() {
				m := make(map[string]LogRowTemplate, len(auditIDs))
				if len(auditIDs) == 0 {
					tplC <- tplRes{m, nil}
					return
				}
				tplRows, err := pool.Query(r.Context(), `
SELECT audit_id::text, COALESCE(version_label,''), COALESCE(subject,''),
       COALESCE(body_text,''), COALESCE(is_html_enabled,false)
FROM notification_svc.audit_template
WHERE audit_id = ANY($1)`, auditIDs)
				if err != nil {
					tplC <- tplRes{m, err}
					return
				}
				defer tplRows.Close()
				for tplRows.Next() {
					var t LogRowTemplate
					if err := tplRows.Scan(&t.AuditID, &t.VersionLabel, &t.Subject, &t.BodyText, &t.IsHTML); err == nil {
						m[t.AuditID] = t
					}
				}
				tplC <- tplRes{m, nil}
			}()

			go func() {
				m := make(map[string][]RetryAttempt, len(outboxIDs))
				histRows, err := pool.Query(r.Context(), `
SELECT outbox_id, attempt_number, processing_status, attempted_at,
       provider_response, provider_message_id
FROM notification_svc.send_history
WHERE outbox_id = ANY($1)
ORDER BY outbox_id, attempt_number ASC`, outboxIDs)
				if err != nil {
					histC <- histRes{m, err}
					return
				}
				defer histRows.Close()
				for histRows.Next() {
					var oid string
					var ra RetryAttempt
					var at time.Time
					if err := histRows.Scan(&oid, &ra.AttemptNumber, &ra.Status, &at, &ra.ProviderResponse, &ra.ProviderMsgID); err == nil {
						ra.AttemptedAt = at.UTC().Format(time.RFC3339)
						m[oid] = append(m[oid], ra)
					}
				}
				histC <- histRes{m, nil}
			}()

			tr := <-tplC
			hr := <-histC
			for i := range result {
				if t, ok := tr.m[result[i].AuditID]; ok {
					tc := t
					result[i].Template = &tc
				}
				if h, ok := hr.m[result[i].OutboxID]; ok {
					result[i].RetryHistory = h
				}
			}
		}

		totalPages := (total + int64(f.Limit) - 1) / int64(f.Limit)
		writeJSON(w, http.StatusOK, LogsResponse{
			Success:    true,
			Rows:       result,
			TotalCount: total,
			Page:       f.Page,
			Limit:      f.Limit,
			TotalPages: totalPages,
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Channel breakdown  —  POST /dash/notification/channel-breakdown
// ─────────────────────────────────────────────────────────────────────────────

type ChannelStat struct {
	Channel      string  `json:"channel"`
	Total        int64   `json:"total"`
	Sent         int64   `json:"sent"`
	Failed       int64   `json:"failed"`
	Dead         int64   `json:"dead"`
	Pending      int64   `json:"pending"`
	AvgRetry     float64 `json:"avg_retry"`
	DeliveryRate float64 `json:"delivery_rate_pct"`
	IsEnabled    bool    `json:"is_enabled"`
}

type ChannelBreakdownResponse struct {
	Success  bool          `json:"success"`
	Channels []ChannelStat `json:"channels"`
}

func GetChannelBreakdown(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		start, end := f.dateWindow()

		q := `
SELECT
    o.channel,
    COUNT(*)                                                                AS total,
    COUNT(*) FILTER (WHERE o.processing_status = 'SENT')                   AS sent,
    COUNT(*) FILTER (WHERE o.processing_status = 'FAILED')                 AS failed,
    COUNT(*) FILTER (WHERE o.processing_status = 'DEAD')                   AS dead,
    COUNT(*) FILTER (WHERE o.processing_status IN ('PENDING','PROCESSING')) AS pending,
    COALESCE(AVG(o.retry_count), 0)                                        AS avg_retry,
    COALESCE(bool_or(nc.is_enabled), false)                                AS is_enabled
FROM notification_svc.outbox o
LEFT JOIN notification_svc.notification_config nc
       ON nc.event_id = o.event_id AND nc.channel = o.channel
WHERE o.created_at BETWEEN $1 AND $2
  AND ($3 = '' OR o.processing_status = $3)
  AND ($4 = '' OR o.correlation_id ILIKE '%' || $4 || '%')
  AND o.retry_count BETWEEN $5 AND $6
GROUP BY o.channel
ORDER BY total DESC
`
		rows, err := pool.Query(context.Background(), q,
			start, end, f.Status, f.CorrelationID, f.MinRetry, f.MaxRetry,
		)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		channels := make([]ChannelStat, 0)
		for rows.Next() {
			var cs ChannelStat
			if err := rows.Scan(
				&cs.Channel, &cs.Total, &cs.Sent, &cs.Failed, &cs.Dead,
				&cs.Pending, &cs.AvgRetry, &cs.IsEnabled,
			); err != nil {
				continue
			}
			if cs.Total > 0 {
				cs.DeliveryRate = float64(cs.Sent) / float64(cs.Total) * 100
			}
			channels = append(channels, cs)
		}
		writeJSON(w, http.StatusOK, ChannelBreakdownResponse{Success: true, Channels: channels})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Hourly trend  —  POST /dash/notification/hourly-trend
// ─────────────────────────────────────────────────────────────────────────────

type HourlyBucket struct {
	Hour   string `json:"hour"` // RFC3339 truncated to hour
	Total  int64  `json:"total"`
	Sent   int64  `json:"sent"`
	Failed int64  `json:"failed"`
	Dead   int64  `json:"dead"`
}

type HourlyTrendResponse struct {
	Success bool           `json:"success"`
	Buckets []HourlyBucket `json:"buckets"`
}

func GetHourlyTrend(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		windowStart := time.Now().Add(-time.Duration(f.HourWindow) * time.Hour)

		q := `
SELECT
    date_trunc('hour', created_at)                             AS hour_bucket,
    COUNT(*)                                                   AS total,
    COUNT(*) FILTER (WHERE processing_status = 'SENT')        AS sent,
    COUNT(*) FILTER (WHERE processing_status = 'FAILED')      AS failed,
    COUNT(*) FILTER (WHERE processing_status = 'DEAD')        AS dead
FROM notification_svc.outbox
WHERE created_at >= $1
  AND ($2 = '' OR channel = $2)
GROUP BY 1
ORDER BY 1 ASC
`
		rows, err := pool.Query(context.Background(), q, windowStart, f.Channel)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		buckets := make([]HourlyBucket, 0)
		for rows.Next() {
			var b HourlyBucket
			var hourBucket time.Time
			if err := rows.Scan(&hourBucket, &b.Total, &b.Sent, &b.Failed, &b.Dead); err != nil {
				continue
			}
			b.Hour = hourBucket.UTC().Format(time.RFC3339)
			buckets = append(buckets, b)
		}
		writeJSON(w, http.StatusOK, HourlyTrendResponse{Success: true, Buckets: buckets})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Top events  —  POST /dash/notification/top-events
// ─────────────────────────────────────────────────────────────────────────────

type EventStat struct {
	EventID      string  `json:"event_id"`
	EventName    string  `json:"event_name"`
	SourceRoute  string  `json:"source_route"`
	Total        int64   `json:"total"`
	Sent         int64   `json:"sent"`
	Failed       int64   `json:"failed"`
	Dead         int64   `json:"dead"`
	DeliveryRate float64 `json:"delivery_rate_pct"`
	AvgRetry     float64 `json:"avg_retry"`
}

type TopEventsResponse struct {
	Success bool        `json:"success"`
	Events  []EventStat `json:"events"`
}

func GetTopEvents(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		start, end := f.dateWindow()

		q := `
SELECT
    o.event_id,
    COALESCE(e.event_display_name, o.event_id) AS event_name,
    COALESCE(e.source_route, '')               AS source_route,
    COUNT(*)                                   AS total,
    COUNT(*) FILTER (WHERE o.processing_status = 'SENT')   AS sent,
    COUNT(*) FILTER (WHERE o.processing_status = 'FAILED') AS failed,
    COUNT(*) FILTER (WHERE o.processing_status = 'DEAD')   AS dead,
    COALESCE(AVG(o.retry_count), 0)            AS avg_retry
FROM notification_svc.outbox o
LEFT JOIN notification_svc.event e ON e.event_id = o.event_id
WHERE o.created_at BETWEEN $1 AND $2
  AND ($3 = '' OR o.channel = $3)
  AND ($4 = '' OR o.processing_status = $4)
GROUP BY o.event_id, e.event_display_name, e.source_route
ORDER BY total DESC
LIMIT 20
`
		rows, err := pool.Query(context.Background(), q, start, end, f.Channel, f.Status)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		events := make([]EventStat, 0)
		for rows.Next() {
			var es EventStat
			if err := rows.Scan(
				&es.EventID, &es.EventName, &es.SourceRoute,
				&es.Total, &es.Sent, &es.Failed, &es.Dead, &es.AvgRetry,
			); err != nil {
				continue
			}
			if es.Total > 0 {
				es.DeliveryRate = float64(es.Sent) / float64(es.Total) * 100
			}
			events = append(events, es)
		}
		writeJSON(w, http.StatusOK, TopEventsResponse{Success: true, Events: events})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Timeline  —  POST /dash/notification/timeline
//    Full event timeline for a given correlation_id
// ─────────────────────────────────────────────────────────────────────────────

type TimelineEvent struct {
	ID          string                 `json:"id"`
	Event       string                 `json:"event"`
	Timestamp   string                 `json:"timestamp"`
	Status      string                 `json:"status"` // "SUCCESS" | "PENDING" | "FAILED"
	Description string                 `json:"description"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

type TimelineResponse struct {
	Success       bool            `json:"success"`
	CorrelationID string          `json:"correlation_id"`
	Events        []TimelineEvent `json:"events"`
}

func GetTimeline(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			CorrelationID string `json:"correlationId"`
		}
		if r.Body == nil {
			errResp(w, http.StatusBadRequest, "correlationId required")
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.CorrelationID == "" {
			errResp(w, http.StatusBadRequest, "correlationId required")
			return
		}

		outboxQ := `
SELECT
    o.outbox_id,
    COALESCE(e.event_display_name, o.event_id) AS event_name,
    o.channel,
    o.processing_status,
    o.created_at,
    o.scheduled_at,
    o.processed_at,
    o.sent_at,
    o.retry_count,
    o.last_error,
    COALESCE(o.recipient_email, o.recipient_phone, o.recipient_user_id, '') AS recipient,
    o.rendered_subject,
    o.priority_level
FROM notification_svc.outbox o
LEFT JOIN notification_svc.event e ON e.event_id = o.event_id
WHERE o.correlation_id = $1
ORDER BY o.created_at ASC
`
		rows, err := pool.Query(context.Background(), outboxQ, body.CorrelationID)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		events := make([]TimelineEvent, 0)
		for rows.Next() {
			var (
				outboxID, eventName, channel, status string
				createdAt                            time.Time
				scheduledAt, processedAt, sentAt     *time.Time
				retryCount, priority                 int
				lastError, recipient, subject        *string
			)
			if err := rows.Scan(
				&outboxID, &eventName, &channel, &status,
				&createdAt, &scheduledAt, &processedAt, &sentAt,
				&retryCount, &lastError, &recipient, &subject, &priority,
			); err != nil {
				continue
			}

			tlStatus := "PENDING"
			if status == "SENT" {
				tlStatus = "SUCCESS"
			} else if status == "FAILED" || status == "DEAD" {
				tlStatus = "FAILED"
			}

			meta := map[string]interface{}{
				"channel":        channel,
				"retry_count":    retryCount,
				"priority_level": priority,
			}
			if recipient != nil {
				meta["recipient"] = *recipient
			}
			if subject != nil {
				meta["subject"] = *subject
			}
			if lastError != nil {
				meta["last_error"] = *lastError
			}

			events = append(events, TimelineEvent{
				ID:          outboxID + "-dispatched",
				Event:       "Dispatched → " + channel,
				Timestamp:   createdAt.UTC().Format(time.RFC3339),
				Status:      "SUCCESS",
				Description: eventName + " dispatched via " + channel,
				Metadata:    meta,
			})
			if processedAt != nil {
				events = append(events, TimelineEvent{
					ID:          outboxID + "-processed",
					Event:       "Processed",
					Timestamp:   processedAt.UTC().Format(time.RFC3339),
					Status:      tlStatus,
					Description: "Worker processed outbox entry → " + status,
					Metadata:    map[string]interface{}{"channel": channel, "status": status},
				})
			}
			if sentAt != nil {
				events = append(events, TimelineEvent{
					ID:          outboxID + "-sent",
					Event:       "Delivered",
					Timestamp:   sentAt.UTC().Format(time.RFC3339),
					Status:      "SUCCESS",
					Description: "Message delivered via " + channel,
					Metadata:    map[string]interface{}{"channel": channel},
				})
			}
		}

		// Enrich with send_history retry attempts
		histQ := `
SELECT
    sh.history_id::text,
    sh.channel,
    sh.processing_status,
    sh.attempted_at,
    sh.attempt_number,
    sh.provider_response,
    sh.provider_message_id,
    COALESCE(sh.recipient_email, sh.recipient_phone, sh.recipient_user_id, '') AS recipient
FROM notification_svc.send_history sh
WHERE sh.correlation_id = $1
ORDER BY sh.attempted_at ASC
`
		histRows, err := pool.Query(context.Background(), histQ, body.CorrelationID)
		if err == nil {
			defer histRows.Close()
			for histRows.Next() {
				var (
					histID, channel, status, recipient string
					attemptedAt                        time.Time
					attemptNum                         int
					provResponse, provMsgID            *string
				)
				if err := histRows.Scan(
					&histID, &channel, &status, &attemptedAt, &attemptNum,
					&provResponse, &provMsgID, &recipient,
				); err != nil {
					continue
				}
				tlStatus := "SUCCESS"
				if status == "FAILED" {
					tlStatus = "FAILED"
				} else if status == "QUEUED" {
					tlStatus = "PENDING"
				}
				meta := map[string]interface{}{
					"channel":   channel,
					"attempt":   attemptNum,
					"recipient": recipient,
				}
				if provResponse != nil {
					meta["provider_response"] = *provResponse
				}
				if provMsgID != nil {
					meta["provider_message_id"] = *provMsgID
				}
				events = append(events, TimelineEvent{
					ID:          histID,
					Event:       "Attempt #" + strconv.Itoa(attemptNum),
					Timestamp:   attemptedAt.UTC().Format(time.RFC3339),
					Status:      tlStatus,
					Description: "Delivery attempt " + strconv.Itoa(attemptNum) + " via " + channel + " → " + status,
					Metadata:    meta,
				})
			}
		}

		// sort by timestamp ascending (insertion sort — small N)
		for i := 1; i < len(events); i++ {
			for j := i; j > 0 && events[j].Timestamp < events[j-1].Timestamp; j-- {
				events[j], events[j-1] = events[j-1], events[j]
			}
		}

		writeJSON(w, http.StatusOK, TimelineResponse{
			Success:       true,
			CorrelationID: body.CorrelationID,
			Events:        events,
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Retry stats  —  POST /dash/notification/retry-stats
// ─────────────────────────────────────────────────────────────────────────────

type RetryDepthRow struct {
	RetryCount  int   `json:"retry_count"`
	Total       int64 `json:"total"`
	StillFailed int64 `json:"still_failed"`
	Recovered   int64 `json:"recovered_to_sent"`
}

type ChannelRetryStat struct {
	Channel   string  `json:"channel"`
	AvgRetry  float64 `json:"avg_retry"`
	MaxRetry  int     `json:"max_retry"`
	DeadCount int64   `json:"dead_count"`
}

type RetryStatsResponse struct {
	Success         bool               `json:"success"`
	DeadLetterCount int64              `json:"dead_letter_count"`
	MaxRetryObs     int                `json:"max_retry_count_observed"`
	AvgRetryCount   float64            `json:"avg_retry_count"`
	RetryDistrib    []RetryDepthRow    `json:"retry_distribution"`
	ChannelRetries  []ChannelRetryStat `json:"channel_retry_summary"`
}

func GetRetryStats(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		start, end := f.dateWindow()

		var deadCount int64
		var maxRetry int
		var avgRetry float64
		summaryQ := `
SELECT
    COUNT(*) FILTER (WHERE processing_status = 'DEAD') AS dead_count,
    COALESCE(MAX(retry_count), 0)                      AS max_retry,
    COALESCE(AVG(retry_count), 0)                      AS avg_retry
FROM notification_svc.outbox
WHERE created_at BETWEEN $1 AND $2
  AND ($3 = '' OR channel = $3)
`
		if err := pool.QueryRow(context.Background(), summaryQ, start, end, f.Channel).Scan(
			&deadCount, &maxRetry, &avgRetry,
		); err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}

		distribQ := `
SELECT
    retry_count,
    COUNT(*)                                                     AS total,
    COUNT(*) FILTER (WHERE processing_status IN ('FAILED','DEAD')) AS still_failed,
    COUNT(*) FILTER (WHERE processing_status = 'SENT')           AS recovered
FROM notification_svc.outbox
WHERE created_at BETWEEN $1 AND $2
  AND ($3 = '' OR channel = $3)
  AND retry_count > 0
GROUP BY retry_count
ORDER BY retry_count ASC
LIMIT 20
`
		distribRows, err := pool.Query(context.Background(), distribQ, start, end, f.Channel)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer distribRows.Close()
		distrib := make([]RetryDepthRow, 0)
		for distribRows.Next() {
			var row RetryDepthRow
			if err := distribRows.Scan(&row.RetryCount, &row.Total, &row.StillFailed, &row.Recovered); err != nil {
				continue
			}
			distrib = append(distrib, row)
		}

		chQ := `
SELECT
    channel,
    COALESCE(AVG(retry_count), 0)                      AS avg_retry,
    COALESCE(MAX(retry_count), 0)                      AS max_retry,
    COUNT(*) FILTER (WHERE processing_status = 'DEAD') AS dead_count
FROM notification_svc.outbox
WHERE created_at BETWEEN $1 AND $2
GROUP BY channel
ORDER BY avg_retry DESC
`
		chRows, err := pool.Query(context.Background(), chQ, start, end)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer chRows.Close()
		chStats := make([]ChannelRetryStat, 0)
		for chRows.Next() {
			var cr ChannelRetryStat
			if err := chRows.Scan(&cr.Channel, &cr.AvgRetry, &cr.MaxRetry, &cr.DeadCount); err != nil {
				continue
			}
			chStats = append(chStats, cr)
		}

		writeJSON(w, http.StatusOK, RetryStatsResponse{
			Success:         true,
			DeadLetterCount: deadCount,
			MaxRetryObs:     maxRetry,
			AvgRetryCount:   avgRetry,
			RetryDistrib:    distrib,
			ChannelRetries:  chStats,
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Send-history detail  —  POST /dash/notification/send-history
//    Provider-level delivery attempts for a given outbox_id or correlation_id
// ─────────────────────────────────────────────────────────────────────────────

type SendHistoryRow struct {
	HistoryID        string  `json:"history_id"`
	OutboxID         string  `json:"outbox_id"`
	CorrelationID    string  `json:"correlation_id"`
	Channel          string  `json:"channel"`
	Status           string  `json:"status"`
	AttemptNumber    int     `json:"attempt_number"`
	AttemptedAt      string  `json:"attempted_at"`
	Recipient        string  `json:"recipient"`
	ProviderResponse *string `json:"provider_response,omitempty"`
	ProviderMsgID    *string `json:"provider_message_id,omitempty"`
	RenderedSubject  *string `json:"rendered_subject,omitempty"`
	SenderName       *string `json:"sender_name,omitempty"`
}

type SendHistoryResponse struct {
	Success bool             `json:"success"`
	Rows    []SendHistoryRow `json:"rows"`
}

func GetSendHistory(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			OutboxID      string `json:"outboxId"`
			CorrelationID string `json:"correlationId"`
		}
		if r.Body == nil {
			errResp(w, http.StatusBadRequest, "outboxId or correlationId required")
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			errResp(w, http.StatusBadRequest, "invalid body")
			return
		}
		if body.OutboxID == "" && body.CorrelationID == "" {
			errResp(w, http.StatusBadRequest, "outboxId or correlationId required")
			return
		}

		q := `
SELECT
    sh.history_id::text,
    sh.outbox_id,
    sh.correlation_id,
    sh.channel,
    sh.processing_status,
    sh.attempt_number,
    sh.attempted_at,
    COALESCE(sh.recipient_email, sh.recipient_phone, sh.recipient_user_id, '') AS recipient,
    sh.provider_response,
    sh.provider_message_id,
    sh.rendered_subject,
    sh.sender_name
FROM notification_svc.send_history sh
WHERE ($1 = '' OR sh.outbox_id = $1)
  AND ($2 = '' OR sh.correlation_id = $2)
ORDER BY sh.attempted_at ASC
LIMIT 200
`
		rows, err := pool.Query(context.Background(), q, body.OutboxID, body.CorrelationID)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		result := make([]SendHistoryRow, 0)
		for rows.Next() {
			var sh SendHistoryRow
			var attemptedAt time.Time
			if err := rows.Scan(
				&sh.HistoryID, &sh.OutboxID, &sh.CorrelationID,
				&sh.Channel, &sh.Status, &sh.AttemptNumber, &attemptedAt,
				&sh.Recipient, &sh.ProviderResponse, &sh.ProviderMsgID,
				&sh.RenderedSubject, &sh.SenderName,
			); err != nil {
				continue
			}
			sh.AttemptedAt = attemptedAt.UTC().Format(time.RFC3339)
			result = append(result, sh)
		}
		writeJSON(w, http.StatusOK, SendHistoryResponse{Success: true, Rows: result})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. Provider stats  —  POST /dash/notification/provider-stats
//    Aggregate delivery metrics per provider (from send_history)
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// 11. Failure reasons  —  POST /dash/notification/failure-reasons
//
// Reads failed rows from outbox.last_error and send_history.provider_response,
// normalises them into short buckets (TIMEOUT, 500_INTERNAL, …) and returns
// a ranked list.  The same data is embedded inside OverviewResponse.
// ─────────────────────────────────────────────────────────────────────────────

type FailureReason struct {
	Error string `json:"error"`
	Count int64  `json:"count"`
}

type FailureReasonsResponse struct {
	Success bool            `json:"success"`
	Reasons []FailureReason `json:"failure_reasons"`
}

// normaliseError maps a raw provider_response / last_error string to a short
// human-readable bucket that the frontend can display in a chart.
func normaliseError(raw string) string {
	if raw == "" {
		return "UNKNOWN"
	}
	switch {
	case contains(raw, "timeout", "timed out", "deadline"):
		return "TIMEOUT"
	case contains(raw, "500", "internal server error", "internal error"):
		return "500_INTERNAL"
	case contains(raw, "400", "invalid payload", "bad request", "invalid", "validation"):
		return "INVALID_PAYLOAD"
	case contains(raw, "429", "rate limit", "too many requests", "throttle"):
		return "RATE_LIMIT"
	case contains(raw, "unknown provider", "no provider", "unsupported channel"):
		return "UNKNOWN_PROVIDER"
	case contains(raw, "401", "403", "unauthorized", "forbidden", "auth"):
		return "AUTH_ERROR"
	case contains(raw, "connection refused", "dial", "network", "eof"):
		return "NETWORK_ERROR"
	case contains(raw, "ses", "sendEmail", "smtp"):
		return "PROVIDER_REJECTED"
	default:
		return "OTHER"
	}
}

// contains is a case-insensitive multi-term OR search.
func contains(s string, terms ...string) bool {
	for _, t := range terms {
		if len(t) > 0 && len(s) >= len(t) {
			si, ti := 0, 0
			for si <= len(s)-len(t) {
				match := true
				for ti = 0; ti < len(t); ti++ {
					sc, tc := s[si+ti], t[ti]
					if sc >= 'A' && sc <= 'Z' {
						sc += 32
					}
					if tc >= 'A' && tc <= 'Z' {
						tc += 32
					}
					if sc != tc {
						match = false
						break
					}
				}
				if match {
					return true
				}
				si++
			}
		}
	}
	return false
}

func fetchFailureReasons(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (FailureReasonsResponse, error) {
	start, end := f.dateWindow()

	// Pull raw error strings from both outbox.last_error and send_history.provider_response
	// for all failed/dead rows in the time window.
	q := `
SELECT COALESCE(o.last_error, '') AS raw_error
FROM notification_svc.outbox o
WHERE o.created_at BETWEEN $1 AND $2
  AND o.processing_status IN ('FAILED', 'DEAD')
  AND ($3 = '' OR o.channel = $3)
UNION ALL
SELECT COALESCE(sh.provider_response, '') AS raw_error
FROM notification_svc.send_history sh
WHERE sh.attempted_at BETWEEN $1 AND $2
  AND sh.processing_status = 'FAILED'
  AND ($3 = '' OR sh.channel = $3)
`
	rows, err := pool.Query(ctx, q, start, end, f.Channel)
	if err != nil {
		return FailureReasonsResponse{Success: true, Reasons: []FailureReason{}}, err
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		bucket := normaliseError(raw)
		counts[bucket]++
	}

	// Sort by count descending
	type kv struct {
		k string
		v int64
	}
	pairs := make([]kv, 0, len(counts))
	for k, v := range counts {
		pairs = append(pairs, kv{k, v})
	}
	for i := 1; i < len(pairs); i++ {
		for j := i; j > 0 && pairs[j].v > pairs[j-1].v; j-- {
			pairs[j], pairs[j-1] = pairs[j-1], pairs[j]
		}
	}

	reasons := make([]FailureReason, 0, len(pairs))
	for _, p := range pairs {
		reasons = append(reasons, FailureReason{Error: p.k, Count: p.v})
	}
	if len(reasons) == 0 {
		reasons = []FailureReason{}
	}
	return FailureReasonsResponse{Success: true, Reasons: reasons}, nil
}

// GetFailureReasons is the standalone handler for POST /dash/notification/failure-reasons.
func GetFailureReasons(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		res, err := fetchFailureReasons(r.Context(), pool, f)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, res)
	}
}

type ProviderStat struct {
	Channel        string  `json:"channel"`
	TotalAttempts  int64   `json:"total_attempts"`
	Succeeded      int64   `json:"succeeded"`
	Failed         int64   `json:"failed"`
	UniqueMessages int64   `json:"unique_messages"`
	AvgAttempts    float64 `json:"avg_attempts_per_message"`
	SuccessRate    float64 `json:"success_rate_pct"`
}

type ProviderStatsResponse struct {
	Success   bool           `json:"success"`
	Providers []ProviderStat `json:"providers"`
}

func GetProviderStats(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		start, end := f.dateWindow()

		q := `
SELECT
    sh.channel,
    COUNT(*)                                                          AS total_attempts,
    COUNT(*) FILTER (WHERE sh.processing_status = 'SENT')            AS succeeded,
    COUNT(*) FILTER (WHERE sh.processing_status = 'FAILED')          AS failed,
    COUNT(DISTINCT sh.outbox_id)                                      AS unique_messages,
    COALESCE(AVG(sh.attempt_number), 1)                               AS avg_attempts
FROM notification_svc.send_history sh
WHERE sh.attempted_at BETWEEN $1 AND $2
  AND ($3 = '' OR sh.channel = $3)
  AND ($4 = '' OR sh.processing_status = $4)
GROUP BY sh.channel
ORDER BY total_attempts DESC
`
		rows, err := pool.Query(context.Background(), q, start, end, f.Channel, f.Status)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		providers := make([]ProviderStat, 0)
		for rows.Next() {
			var ps ProviderStat
			if err := rows.Scan(
				&ps.Channel, &ps.TotalAttempts, &ps.Succeeded, &ps.Failed,
				&ps.UniqueMessages, &ps.AvgAttempts,
			); err != nil {
				continue
			}
			if ps.TotalAttempts > 0 {
				ps.SuccessRate = float64(ps.Succeeded) / float64(ps.TotalAttempts) * 100
			}
			providers = append(providers, ps)
		}
		writeJSON(w, http.StatusOK, ProviderStatsResponse{Success: true, Providers: providers})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. Event config  —  POST /dash/notification/event-config
//     List all events with their per-channel configuration
// ─────────────────────────────────────────────────────────────────────────────

type EventConfigChannel struct {
	Channel       string `json:"channel"`
	IsEnabled     bool   `json:"is_enabled"`
	RetryMax      int    `json:"retry_max"`
	PriorityLevel int    `json:"priority_level"`
}

type EventConfigRow struct {
	EventID     string               `json:"event_id"`
	EventName   string               `json:"event_name"`
	SourceRoute string               `json:"source_route"`
	IsActive    bool                 `json:"is_active"`
	Channels    []EventConfigChannel `json:"channels"`
}

type EventConfigResponse struct {
	Success bool             `json:"success"`
	Events  []EventConfigRow `json:"events"`
}

func GetEventConfig(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := `
SELECT
    e.event_id,
    COALESCE(e.event_display_name, e.event_id) AS event_name,
    COALESCE(e.source_route, '')               AS source_route,
    e.is_active,
    nc.channel,
    nc.is_enabled,
    nc.retry_max,
    nc.priority_level
FROM notification_svc.event e
LEFT JOIN notification_svc.notification_config nc ON nc.event_id = e.event_id
ORDER BY e.event_id, nc.channel
`
		rows, err := pool.Query(context.Background(), q)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		// Group channels per event
		eventMap := make(map[string]*EventConfigRow)
		eventOrder := make([]string, 0)
		for rows.Next() {
			var (
				eventID, eventName, sourceRoute string
				isActive                        bool
				channel                         *string
				isEnabled                       *bool
				retryMax, priority              *int
			)
			if err := rows.Scan(
				&eventID, &eventName, &sourceRoute, &isActive,
				&channel, &isEnabled, &retryMax, &priority,
			); err != nil {
				continue
			}
			if _, ok := eventMap[eventID]; !ok {
				eventMap[eventID] = &EventConfigRow{
					EventID:     eventID,
					EventName:   eventName,
					SourceRoute: sourceRoute,
					IsActive:    isActive,
					Channels:    make([]EventConfigChannel, 0),
				}
				eventOrder = append(eventOrder, eventID)
			}
			if channel != nil {
				chCfg := EventConfigChannel{Channel: *channel}
				if isEnabled != nil {
					chCfg.IsEnabled = *isEnabled
				}
				if retryMax != nil {
					chCfg.RetryMax = *retryMax
				}
				if priority != nil {
					chCfg.PriorityLevel = *priority
				}
				eventMap[eventID].Channels = append(eventMap[eventID].Channels, chCfg)
			}
		}

		result := make([]EventConfigRow, 0, len(eventOrder))
		for _, id := range eventOrder {
			result = append(result, *eventMap[id])
		}
		writeJSON(w, http.StatusOK, EventConfigResponse{Success: true, Events: result})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// OVERVIEW  —  POST /dash/notification/overview
//
// Single endpoint that runs all 10 data fetches concurrently and returns
// everything in one JSON payload. Useful for dashboards that need to load
// the full page in a single round-trip.
//
// Request body is the standard filterRequest (same as all other endpoints).
// Timeline and SendHistory are excluded from overview (they require a specific
// correlation_id / outbox_id) but all aggregate sections are included.
// ─────────────────────────────────────────────────────────────────────────────

type OverviewResponse struct {
	Success          bool                     `json:"success"`
	KPI              KPIResponse              `json:"kpi"`
	ChannelBreakdown ChannelBreakdownResponse `json:"channel_breakdown"`
	HourlyTrend      HourlyTrendResponse      `json:"hourly_trend"`
	TopEvents        TopEventsResponse        `json:"top_events"`
	RetryStats       RetryStatsResponse       `json:"retry_stats"`
	Logs             LogsResponse             `json:"logs"`
	ProviderStats    ProviderStatsResponse    `json:"provider_stats"`
	EventConfig      EventConfigResponse      `json:"event_config"`
	FailureReasons   FailureReasonsResponse   `json:"failure_reasons"`
}

// fetchKPI, fetchChannelBreakdown, … are thin helpers that run a single
// section's queries and write the result into a pre-allocated struct.
// Each runs inside its own goroutine in GetOverview.

func fetchKPI(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (KPIResponse, error) {
	start, end := f.dateWindow()
	q := `
SELECT
    COUNT(*),
    COUNT(*) FILTER (WHERE processing_status = 'SENT'),
    COUNT(*) FILTER (WHERE processing_status = 'FAILED'),
    COUNT(*) FILTER (WHERE processing_status = 'DEAD'),
    COUNT(*) FILTER (WHERE processing_status IN ('PENDING','PROCESSING')),
    COUNT(*) FILTER (WHERE retry_count > 0 AND processing_status NOT IN ('SENT','DEAD')),
    COUNT(*) FILTER (WHERE channel = 'EMAIL'),
    COUNT(*) FILTER (WHERE channel = 'SMS'),
    COUNT(*) FILTER (WHERE channel = 'PUSH'),
    COUNT(*) FILTER (WHERE channel = 'WHATSAPP'),
    COALESCE(AVG(retry_count), 0),
    COALESCE(AVG(EXTRACT(EPOCH FROM (sent_at - scheduled_at)) * 1000) FILTER (WHERE sent_at IS NOT NULL), 0)
FROM notification_svc.outbox
WHERE created_at BETWEEN $1 AND $2
  AND ($3 = '' OR channel = $3)
  AND ($4 = '' OR processing_status = $4)
  AND ($5 = '' OR correlation_id ILIKE '%' || $5 || '%')
  AND ($6 = '' OR recipient_email ILIKE '%' || $6 || '%' OR recipient_phone ILIKE '%' || $6 || '%')
  AND retry_count BETWEEN $7 AND $8
`
	var kpi KPIResponse
	kpi.Success = true
	err := pool.QueryRow(ctx, q,
		start, end, f.Channel, f.Status, f.CorrelationID, f.Recipient, f.MinRetry, f.MaxRetry,
	).Scan(
		&kpi.TotalDispatched, &kpi.TotalSent, &kpi.TotalFailed, &kpi.TotalDead,
		&kpi.TotalPending, &kpi.TotalRetrying,
		&kpi.ChannelEmail, &kpi.ChannelSMS, &kpi.ChannelPush, &kpi.ChannelWhatsApp,
		&kpi.AvgRetryCount, &kpi.AvgLatencyMs,
	)
	if err != nil {
		return kpi, err
	}
	if kpi.TotalDispatched > 0 {
		kpi.DeliveryRate = float64(kpi.TotalSent) / float64(kpi.TotalDispatched) * 100
		kpi.FailureRate = float64(kpi.TotalFailed+kpi.TotalDead) / float64(kpi.TotalDispatched) * 100
	}
	return kpi, nil
}

func fetchChannelBreakdown(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (ChannelBreakdownResponse, error) {
	start, end := f.dateWindow()
	q := `
SELECT
    o.channel,
    COUNT(*),
    COUNT(*) FILTER (WHERE o.processing_status = 'SENT'),
    COUNT(*) FILTER (WHERE o.processing_status = 'FAILED'),
    COUNT(*) FILTER (WHERE o.processing_status = 'DEAD'),
    COUNT(*) FILTER (WHERE o.processing_status IN ('PENDING','PROCESSING')),
    COALESCE(AVG(o.retry_count), 0),
    COALESCE(bool_or(nc.is_enabled), false)
FROM notification_svc.outbox o
LEFT JOIN notification_svc.notification_config nc ON nc.event_id = o.event_id AND nc.channel = o.channel
WHERE o.created_at BETWEEN $1 AND $2
  AND ($3 = '' OR o.processing_status = $3)
  AND ($4 = '' OR o.correlation_id ILIKE '%' || $4 || '%')
  AND o.retry_count BETWEEN $5 AND $6
GROUP BY o.channel
ORDER BY 2 DESC
`
	rows, err := pool.Query(ctx, q, start, end, f.Status, f.CorrelationID, f.MinRetry, f.MaxRetry)
	if err != nil {
		return ChannelBreakdownResponse{Success: true, Channels: []ChannelStat{}}, err
	}
	defer rows.Close()
	channels := make([]ChannelStat, 0)
	for rows.Next() {
		var cs ChannelStat
		if err := rows.Scan(&cs.Channel, &cs.Total, &cs.Sent, &cs.Failed, &cs.Dead, &cs.Pending, &cs.AvgRetry, &cs.IsEnabled); err != nil {
			continue
		}
		if cs.Total > 0 {
			cs.DeliveryRate = float64(cs.Sent) / float64(cs.Total) * 100
		}
		channels = append(channels, cs)
	}
	return ChannelBreakdownResponse{Success: true, Channels: channels}, nil
}

func fetchHourlyTrend(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (HourlyTrendResponse, error) {
	windowStart := time.Now().Add(-time.Duration(f.HourWindow) * time.Hour)
	q := `
SELECT date_trunc('hour', created_at), COUNT(*),
       COUNT(*) FILTER (WHERE processing_status = 'SENT'),
       COUNT(*) FILTER (WHERE processing_status = 'FAILED'),
       COUNT(*) FILTER (WHERE processing_status = 'DEAD')
FROM notification_svc.outbox
WHERE created_at >= $1 AND ($2 = '' OR channel = $2)
GROUP BY 1 ORDER BY 1 ASC
`
	rows, err := pool.Query(ctx, q, windowStart, f.Channel)
	if err != nil {
		return HourlyTrendResponse{Success: true, Buckets: []HourlyBucket{}}, err
	}
	defer rows.Close()
	buckets := make([]HourlyBucket, 0)
	for rows.Next() {
		var b HourlyBucket
		var h time.Time
		if err := rows.Scan(&h, &b.Total, &b.Sent, &b.Failed, &b.Dead); err != nil {
			continue
		}
		b.Hour = h.UTC().Format(time.RFC3339)
		buckets = append(buckets, b)
	}
	return HourlyTrendResponse{Success: true, Buckets: buckets}, nil
}

func fetchTopEvents(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (TopEventsResponse, error) {
	start, end := f.dateWindow()
	q := `
SELECT o.event_id, COALESCE(e.event_display_name, o.event_id), COALESCE(e.source_route, ''),
       COUNT(*),
       COUNT(*) FILTER (WHERE o.processing_status = 'SENT'),
       COUNT(*) FILTER (WHERE o.processing_status = 'FAILED'),
       COUNT(*) FILTER (WHERE o.processing_status = 'DEAD'),
       COALESCE(AVG(o.retry_count), 0)
FROM notification_svc.outbox o
LEFT JOIN notification_svc.event e ON e.event_id = o.event_id
WHERE o.created_at BETWEEN $1 AND $2 AND ($3 = '' OR o.channel = $3) AND ($4 = '' OR o.processing_status = $4)
GROUP BY o.event_id, e.event_display_name, e.source_route
ORDER BY 4 DESC LIMIT 20
`
	rows, err := pool.Query(ctx, q, start, end, f.Channel, f.Status)
	if err != nil {
		return TopEventsResponse{Success: true, Events: []EventStat{}}, err
	}
	defer rows.Close()
	events := make([]EventStat, 0)
	for rows.Next() {
		var es EventStat
		if err := rows.Scan(&es.EventID, &es.EventName, &es.SourceRoute, &es.Total, &es.Sent, &es.Failed, &es.Dead, &es.AvgRetry); err != nil {
			continue
		}
		if es.Total > 0 {
			es.DeliveryRate = float64(es.Sent) / float64(es.Total) * 100
		}
		events = append(events, es)
	}
	return TopEventsResponse{Success: true, Events: events}, nil
}

func fetchRetryStats(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (RetryStatsResponse, error) {
	start, end := f.dateWindow()
	var deadCount int64
	var maxRetry int
	var avgRetry float64
	if err := pool.QueryRow(ctx, `
SELECT COUNT(*) FILTER (WHERE processing_status = 'DEAD'), COALESCE(MAX(retry_count),0), COALESCE(AVG(retry_count),0)
FROM notification_svc.outbox WHERE created_at BETWEEN $1 AND $2 AND ($3='' OR channel=$3)
`, start, end, f.Channel).Scan(&deadCount, &maxRetry, &avgRetry); err != nil {
		return RetryStatsResponse{Success: true}, err
	}
	distribRows, err := pool.Query(ctx, `
SELECT retry_count, COUNT(*),
       COUNT(*) FILTER (WHERE processing_status IN ('FAILED','DEAD')),
       COUNT(*) FILTER (WHERE processing_status = 'SENT')
FROM notification_svc.outbox
WHERE created_at BETWEEN $1 AND $2 AND ($3='' OR channel=$3) AND retry_count > 0
GROUP BY retry_count ORDER BY retry_count ASC LIMIT 20
`, start, end, f.Channel)
	if err != nil {
		return RetryStatsResponse{Success: true}, err
	}
	defer distribRows.Close()
	distrib := make([]RetryDepthRow, 0)
	for distribRows.Next() {
		var row RetryDepthRow
		if err := distribRows.Scan(&row.RetryCount, &row.Total, &row.StillFailed, &row.Recovered); err != nil {
			continue
		}
		distrib = append(distrib, row)
	}
	chRows, err := pool.Query(ctx, `
SELECT channel, COALESCE(AVG(retry_count),0), COALESCE(MAX(retry_count),0),
       COUNT(*) FILTER (WHERE processing_status='DEAD')
FROM notification_svc.outbox WHERE created_at BETWEEN $1 AND $2 GROUP BY channel ORDER BY 2 DESC
`, start, end)
	if err != nil {
		return RetryStatsResponse{Success: true}, err
	}
	defer chRows.Close()
	chStats := make([]ChannelRetryStat, 0)
	for chRows.Next() {
		var cr ChannelRetryStat
		if err := chRows.Scan(&cr.Channel, &cr.AvgRetry, &cr.MaxRetry, &cr.DeadCount); err != nil {
			continue
		}
		chStats = append(chStats, cr)
	}
	return RetryStatsResponse{
		Success: true, DeadLetterCount: deadCount, MaxRetryObs: maxRetry, AvgRetryCount: avgRetry,
		RetryDistrib: distrib, ChannelRetries: chStats,
	}, nil
}

func fetchLogs(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (LogsResponse, error) {
	start, end := f.dateWindow()
	offset := (f.Page - 1) * f.Limit

	// Choose the SELECT list based on whether the caller wants the heavy details.
	// When log_info=false we skip variables_payload, template and retry_history
	// entirely — saving both the DB column fetch and the two batch sub-queries.
	varCol := "'{}'"
	if f.LogInfo {
		varCol = "COALESCE(o.variables_payload::text, '{}')"
	}

	baseWhere := `
FROM notification_svc.outbox o
JOIN notification_svc.event e ON e.event_id = o.event_id
WHERE o.created_at BETWEEN $1 AND $2
  AND ($3='' OR o.channel=$3) AND ($4='' OR o.processing_status=$4)
  AND ($5='' OR o.event_id=$5 OR e.event_display_name ILIKE '%'||$5||'%')
  AND ($6='' OR o.correlation_id ILIKE '%'||$6||'%')
  AND ($7='' OR o.recipient_email ILIKE '%'||$7||'%' OR o.recipient_phone ILIKE '%'||$7||'%')
  AND o.retry_count BETWEEN $8 AND $9`

	var total int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) `+baseWhere,
		start, end, f.Channel, f.Status, f.EventType, f.CorrelationID, f.Recipient, f.MinRetry, f.MaxRetry,
	).Scan(&total); err != nil {
		return LogsResponse{Success: true}, err
	}

	rowQ := `
SELECT o.outbox_id, o.correlation_id, COALESCE(e.event_display_name, o.event_id),
       o.channel, COALESCE(o.recipient_email, o.recipient_phone, o.recipient_user_id, ''),
       o.processing_status, o.retry_count,
       CASE WHEN o.sent_at IS NOT NULL THEN EXTRACT(EPOCH FROM (o.sent_at - o.scheduled_at))*1000 ELSE NULL END,
       o.created_at, o.processed_at, o.last_error, o.audit_id::text,
       o.sender_name, o.sender_email, o.rendered_subject, o.priority_level,
       ` + varCol + ` ` + baseWhere + `
ORDER BY o.created_at DESC LIMIT $10 OFFSET $11`

	rows, err := pool.Query(ctx, rowQ,
		start, end, f.Channel, f.Status, f.EventType, f.CorrelationID, f.Recipient,
		f.MinRetry, f.MaxRetry, f.Limit, offset,
	)
	if err != nil {
		return LogsResponse{Success: true}, err
	}
	defer rows.Close()

	result := make([]LogRow, 0, f.Limit)
	for rows.Next() {
		var row LogRow
		var createdAt time.Time
		var processedAt *time.Time
		var latencyMs *float64
		var variablesRaw string
		if err := rows.Scan(
			&row.OutboxID, &row.CorrelationID, &row.EventName,
			&row.Channel, &row.Recipient, &row.Status,
			&row.RetryCount, &latencyMs,
			&createdAt, &processedAt, &row.ErrorMessage,
			&row.AuditID, &row.SenderName, &row.SenderEmail,
			&row.RenderedSubject, &row.PriorityLevel,
			&variablesRaw,
		); err != nil {
			continue
		}
		row.LatencyMs = latencyMs
		row.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		if processedAt != nil {
			s := processedAt.UTC().Format(time.RFC3339)
			row.UpdatedAt = &s
		}
		// Only unmarshal variables_payload when log_info=true (otherwise it's '{}').
		if f.LogInfo {
			row.VariablesPayload = make(map[string]interface{})
			_ = json.Unmarshal([]byte(variablesRaw), &row.VariablesPayload)
		} else {
			row.VariablesPayload = map[string]interface{}{}
		}
		// RetryHistory defaults to empty slice (never nil in JSON)
		row.RetryHistory = []RetryAttempt{}
		result = append(result, row)
	}
	rows.Close() // close early so the connection returns to the pool before batch queries

	// ── Batch enrich: only when log_info=true ────────────────────────────────
	// Instead of N×2 serial sub-queries we fire exactly 2 parallel queries for
	// the whole page, then assemble results in Go maps.
	if f.LogInfo && len(result) > 0 {
		// Collect outbox_ids and audit_ids for the page.
		outboxIDs := make([]string, len(result))
		auditIDs := make([]string, 0, len(result))
		auditIDSet := make(map[string]bool, len(result))
		for i, r := range result {
			outboxIDs[i] = r.OutboxID
			if r.AuditID != "" && !auditIDSet[r.AuditID] {
				auditIDs = append(auditIDs, r.AuditID)
				auditIDSet[r.AuditID] = true
			}
		}

		type tplFetchRes struct {
			m map[string]LogRowTemplate
			e error
		}
		type histFetchRes struct {
			m map[string][]RetryAttempt
			e error
		}
		tplC := make(chan tplFetchRes, 1)
		histC := make(chan histFetchRes, 1)

		// Goroutine 1: fetch all templates for this page in one query.
		go func() {
			m := make(map[string]LogRowTemplate, len(auditIDs))
			if len(auditIDs) == 0 {
				tplC <- tplFetchRes{m, nil}
				return
			}
			tplRows, err := pool.Query(ctx, `
SELECT audit_id::text, COALESCE(version_label,''), COALESCE(subject,''),
       COALESCE(body_text,''), COALESCE(is_html_enabled,false)
FROM notification_svc.audit_template
WHERE audit_id = ANY($1)`, auditIDs)
			if err != nil {
				tplC <- tplFetchRes{m, err}
				return
			}
			defer tplRows.Close()
			for tplRows.Next() {
				var t LogRowTemplate
				if err := tplRows.Scan(&t.AuditID, &t.VersionLabel, &t.Subject, &t.BodyText, &t.IsHTML); err == nil {
					m[t.AuditID] = t
				}
			}
			tplC <- tplFetchRes{m, nil}
		}()

		// Goroutine 2: fetch all send_history rows for this page in one query.
		go func() {
			m := make(map[string][]RetryAttempt, len(outboxIDs))
			histRows, err := pool.Query(ctx, `
SELECT outbox_id, attempt_number, processing_status, attempted_at,
       provider_response, provider_message_id
FROM notification_svc.send_history
WHERE outbox_id = ANY($1)
ORDER BY outbox_id, attempt_number ASC`, outboxIDs)
			if err != nil {
				histC <- histFetchRes{m, err}
				return
			}
			defer histRows.Close()
			for histRows.Next() {
				var oid string
				var ra RetryAttempt
				var at time.Time
				if err := histRows.Scan(&oid, &ra.AttemptNumber, &ra.Status, &at, &ra.ProviderResponse, &ra.ProviderMsgID); err == nil {
					ra.AttemptedAt = at.UTC().Format(time.RFC3339)
					m[oid] = append(m[oid], ra)
				}
			}
			histC <- histFetchRes{m, nil}
		}()

		// Wait for both batch queries and assemble.
		tplRes := <-tplC
		histRes := <-histC

		for i := range result {
			if t, ok := tplRes.m[result[i].AuditID]; ok {
				tc := t // copy
				result[i].Template = &tc
			}
			if h, ok := histRes.m[result[i].OutboxID]; ok {
				result[i].RetryHistory = h
			}
		}
	}

	totalPages := (total + int64(f.Limit) - 1) / int64(f.Limit)
	return LogsResponse{Success: true, Rows: result, TotalCount: total, Page: f.Page, Limit: f.Limit, TotalPages: totalPages}, nil
}

func fetchProviderStats(ctx context.Context, pool *pgxpool.Pool, f filterRequest) (ProviderStatsResponse, error) {
	start, end := f.dateWindow()
	rows, err := pool.Query(ctx, `
SELECT sh.channel,
       COUNT(*), COUNT(*) FILTER (WHERE sh.processing_status='SENT'),
       COUNT(*) FILTER (WHERE sh.processing_status='FAILED'),
       COUNT(DISTINCT sh.outbox_id), COALESCE(AVG(sh.attempt_number),1)
FROM notification_svc.send_history sh
WHERE sh.attempted_at BETWEEN $1 AND $2
  AND ($3='' OR sh.channel=$3) AND ($4='' OR sh.processing_status=$4)
GROUP BY sh.channel ORDER BY 2 DESC
`, start, end, f.Channel, f.Status)
	if err != nil {
		return ProviderStatsResponse{Success: true, Providers: []ProviderStat{}}, err
	}
	defer rows.Close()
	providers := make([]ProviderStat, 0)
	for rows.Next() {
		var ps ProviderStat
		if err := rows.Scan(&ps.Channel, &ps.TotalAttempts, &ps.Succeeded, &ps.Failed, &ps.UniqueMessages, &ps.AvgAttempts); err != nil {
			continue
		}
		if ps.TotalAttempts > 0 {
			ps.SuccessRate = float64(ps.Succeeded) / float64(ps.TotalAttempts) * 100
		}
		providers = append(providers, ps)
	}
	return ProviderStatsResponse{Success: true, Providers: providers}, nil
}

func fetchEventConfig(ctx context.Context, pool *pgxpool.Pool) (EventConfigResponse, error) {
	rows, err := pool.Query(ctx, `
SELECT e.event_id, COALESCE(e.event_display_name, e.event_id), COALESCE(e.source_route,''), e.is_active,
       nc.channel, nc.is_enabled, nc.retry_max, nc.priority_level
FROM notification_svc.event e
LEFT JOIN notification_svc.notification_config nc ON nc.event_id = e.event_id
ORDER BY e.event_id, nc.channel
`)
	if err != nil {
		return EventConfigResponse{Success: true, Events: []EventConfigRow{}}, err
	}
	defer rows.Close()
	eventMap := make(map[string]*EventConfigRow)
	eventOrder := make([]string, 0)
	for rows.Next() {
		var (
			eventID, eventName, sourceRoute string
			isActive                        bool
			channel                         *string
			isEnabled                       *bool
			retryMax, priority              *int
		)
		if err := rows.Scan(&eventID, &eventName, &sourceRoute, &isActive, &channel, &isEnabled, &retryMax, &priority); err != nil {
			continue
		}
		if _, ok := eventMap[eventID]; !ok {
			eventMap[eventID] = &EventConfigRow{EventID: eventID, EventName: eventName, SourceRoute: sourceRoute, IsActive: isActive, Channels: make([]EventConfigChannel, 0)}
			eventOrder = append(eventOrder, eventID)
		}
		if channel != nil {
			chCfg := EventConfigChannel{Channel: *channel}
			if isEnabled != nil {
				chCfg.IsEnabled = *isEnabled
			}
			if retryMax != nil {
				chCfg.RetryMax = *retryMax
			}
			if priority != nil {
				chCfg.PriorityLevel = *priority
			}
			eventMap[eventID].Channels = append(eventMap[eventID].Channels, chCfg)
		}
	}
	result := make([]EventConfigRow, 0, len(eventOrder))
	for _, id := range eventOrder {
		result = append(result, *eventMap[id])
	}
	return EventConfigResponse{Success: true, Events: result}, nil
}

// GetOverview runs all 10 sections concurrently and returns one combined payload.
// POST /dash/notification/overview
// Body: same filterRequest as every other endpoint.
func GetOverview(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := decodeFilter(r)
		if err != nil {
			errResp(w, http.StatusBadRequest, constants.ErrInvalidFilter+err.Error())
			return
		}
		ctx := r.Context()

		type result struct {
			kpi       KPIResponse
			chBreak   ChannelBreakdownResponse
			hourly    HourlyTrendResponse
			topEvt    TopEventsResponse
			retry     RetryStatsResponse
			logs      LogsResponse
			provider  ProviderStatsResponse
			evtConfig EventConfigResponse
			failures  FailureReasonsResponse
			err       error
		}

		ch := make(chan result, 1)
		go func() {
			var res result

			// Fan out ALL fetches concurrently — KPI included.
			type kpiRes struct {
				v KPIResponse
				e error
			}
			type chBreakRes struct {
				v ChannelBreakdownResponse
				e error
			}
			type hourlyRes struct {
				v HourlyTrendResponse
				e error
			}
			type topEvtRes struct {
				v TopEventsResponse
				e error
			}
			type retryRes struct {
				v RetryStatsResponse
				e error
			}
			type logsRes struct {
				v LogsResponse
				e error
			}
			type provRes struct {
				v ProviderStatsResponse
				e error
			}
			type cfgRes struct {
				v EventConfigResponse
				e error
			}
			type failRes struct {
				v FailureReasonsResponse
				e error
			}

			kpiC := make(chan kpiRes, 1)
			chBreakC := make(chan chBreakRes, 1)
			hourlyC := make(chan hourlyRes, 1)
			topEvtC := make(chan topEvtRes, 1)
			retryC := make(chan retryRes, 1)
			logsC := make(chan logsRes, 1)
			provC := make(chan provRes, 1)
			cfgC := make(chan cfgRes, 1)
			failC := make(chan failRes, 1)

			go func() { v, e := fetchKPI(ctx, pool, f); kpiC <- kpiRes{v, e} }()
			go func() { v, e := fetchChannelBreakdown(ctx, pool, f); chBreakC <- chBreakRes{v, e} }()
			go func() { v, e := fetchHourlyTrend(ctx, pool, f); hourlyC <- hourlyRes{v, e} }()
			go func() { v, e := fetchTopEvents(ctx, pool, f); topEvtC <- topEvtRes{v, e} }()
			go func() { v, e := fetchRetryStats(ctx, pool, f); retryC <- retryRes{v, e} }()
			go func() { v, e := fetchLogs(ctx, pool, f); logsC <- logsRes{v, e} }()
			go func() { v, e := fetchProviderStats(ctx, pool, f); provC <- provRes{v, e} }()
			go func() { v, e := fetchEventConfig(ctx, pool); cfgC <- cfgRes{v, e} }()
			go func() { v, e := fetchFailureReasons(ctx, pool, f); failC <- failRes{v, e} }()

			r0 := <-kpiC
			res.kpi = r0.v
			if r0.e != nil {
				res.err = r0.e
			}
			r1 := <-chBreakC
			res.chBreak = r1.v
			r2 := <-hourlyC
			res.hourly = r2.v
			r3 := <-topEvtC
			res.topEvt = r3.v
			r4 := <-retryC
			res.retry = r4.v
			r5 := <-logsC
			res.logs = r5.v
			r6 := <-provC
			res.provider = r6.v
			r7 := <-cfgC
			res.evtConfig = r7.v
			r8 := <-failC
			res.failures = r8.v

			ch <- res
		}()

		res := <-ch
		if res.err != nil {
			errResp(w, http.StatusInternalServerError, res.err.Error())
			return
		}

		writeJSON(w, http.StatusOK, OverviewResponse{
			Success:          true,
			KPI:              res.kpi,
			ChannelBreakdown: res.chBreak,
			HourlyTrend:      res.hourly,
			TopEvents:        res.topEvt,
			RetryStats:       res.retry,
			Logs:             res.logs,
			ProviderStats:    res.provider,
			EventConfig:      res.evtConfig,
			FailureReasons:   res.failures,
		})
	}
}
