package auditutil

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"strings"
	"time"

	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	TableExposure            = "public.auditactionexposure"
	TableExposureDownloads   = "public.auditactionexposuredownloads"
	TableExposureBucketing   = "public.auditactionexposurebucketing"
	TableHedgeProposal       = "public.auditactionhedgeproposal"
	TableHedgeLink           = "public.auditactionhedgelink"
	TableForwardBooking      = "public.auditactionforwardbooking"
	TableForwardDownloads    = "public.auditactionforwardbookingdownloads"
	TableForwardMTM          = "public.auditactionforwardmtm"
	TableForwardMTMDownloads = "public.auditactionforwardmtmdownloads"
	TableForwardCancellation = "public.auditactionforwardcancellation"
	TableForwardRollover     = "public.auditactionforwardrollover"
)

type ExecContext interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) error
}

type sqlExecAdapter struct {
	db *sql.DB
}

func (a sqlExecAdapter) ExecContext(ctx context.Context, query string, args ...interface{}) error {
	_, err := a.db.ExecContext(ctx, query, args...)
	return err
}

type pgxExecAdapter struct {
	pool *pgxpool.Pool
}

func (a pgxExecAdapter) ExecContext(ctx context.Context, query string, args ...interface{}) error {
	_, err := a.pool.Exec(ctx, query, args...)
	return err
}

type execAttempt struct {
	query string
	args  []interface{}
}

func execFirstSuccess(ctx context.Context, exec ExecContext, attempts []execAttempt) error {
	var lastErr error
	for _, attempt := range attempts {
		if err := exec.ExecContext(ctx, attempt.query, attempt.args...); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

func Actor(userID string) string {
	userID = strings.TrimSpace(userID)
	for _, session := range auth.GetActiveSessions() {
		if session.UserID == userID {
			if name := strings.TrimSpace(session.Name); name != "" {
				return name
			}
			if email := strings.TrimSpace(session.Email); email != "" {
				return email
			}
			break
		}
	}
	return userID
}

func ActorFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if userID, ok := ctx.Value("user_id").(string); ok {
		return Actor(userID)
	}
	return ""
}

func NullIfBlank(value string) interface{} {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func FileName(uploadS3Key string) interface{} {
	uploadS3Key = strings.TrimSpace(uploadS3Key)
	if uploadS3Key == "" {
		return nil
	}
	name := strings.TrimSpace(path.Base(uploadS3Key))
	if name == "." || name == "/" || name == "" {
		return nil
	}
	return name
}

func JSONValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return string(bytes)
}

func FetchRowSnapshot(ctx context.Context, db *sql.DB, tableName, parentColumn, parentID string) map[string]interface{} {
	parentID = strings.TrimSpace(parentID)
	if db == nil || tableName == "" || parentColumn == "" || parentID == "" {
		return nil
	}

	query := fmt.Sprintf(`SELECT row_to_json(t) FROM (SELECT * FROM %s WHERE %s = $1 LIMIT 1) t`, tableName, parentColumn)
	var raw []byte
	if err := db.QueryRowContext(ctx, query, parentID).Scan(&raw); err != nil || len(raw) == 0 {
		return nil
	}

	var snapshot map[string]interface{}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil
	}
	return snapshot
}

func FetchRowSnapshotPGX(ctx context.Context, pool *pgxpool.Pool, tableName, parentColumn, parentID string) map[string]interface{} {
	parentID = strings.TrimSpace(parentID)
	if pool == nil || tableName == "" || parentColumn == "" || parentID == "" {
		return nil
	}

	query := fmt.Sprintf(`SELECT row_to_json(t) FROM (SELECT * FROM %s WHERE %s = $1 LIMIT 1) t`, tableName, parentColumn)
	var raw []byte
	if err := pool.QueryRow(ctx, query, parentID).Scan(&raw); err != nil || len(raw) == 0 {
		return nil
	}

	var snapshot map[string]interface{}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil
	}
	return snapshot
}

// ActionParams groups the audit-action parameters to keep function signatures short.
type ActionParams struct {
	TableName    string
	ParentColumn string
	ParentID     string
	ActionType   string
	Status       string
	Reason       string
	RequestedBy  string
	OldValues    interface{}
	NewValues    interface{}
}

// DecisionParams groups the audit-decision parameters.
type DecisionParams struct {
	TableName    string
	ParentColumn string
	ParentID     string
	Status       string
	CheckerBy    string
	Comment      string
}

// DownloadParams groups the audit-download parameters.
type DownloadParams struct {
	TableName    string
	ParentColumn string
	ParentID     string
	RequestedBy  string
	UploadS3Key  string
	ExtraColumns map[string]string
}

func RecordAction(ctx context.Context, db *sql.DB, p ActionParams) {
	p.ParentID = strings.TrimSpace(p.ParentID)
	p.RequestedBy = strings.TrimSpace(p.RequestedBy)
	if p.ParentID == "" || p.RequestedBy == "" || db == nil {
		return
	}
	attempts := []execAttempt{
		{
			query: fmt.Sprintf(`
			INSERT INTO %s (%s, actiontype, processing_status, reason, requested_by, requested_at, old_values, new_values, change_summary)
			VALUES ($1, $2, $3, $4, $5, now(), $6, $7, $8)
		`, p.TableName, p.ParentColumn),
			args: []interface{}{p.ParentID, p.ActionType, p.Status, NullIfBlank(p.Reason), p.RequestedBy, JSONValue(p.OldValues), JSONValue(p.NewValues), JSONValue(BuildChangeSummary(p.OldValues, p.NewValues))},
		},
		{
			query: fmt.Sprintf(`
			INSERT INTO %s (%s, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1, $2, $3, $4, $5, now())
		`, p.TableName, p.ParentColumn),
			args: []interface{}{p.ParentID, p.ActionType, p.Status, NullIfBlank(p.Reason), p.RequestedBy},
		},
	}
	if err := execFirstSuccess(ctx, sqlExecAdapter{db: db}, attempts); err != nil {
		logger.LogError("fx audit insert failed table=%s parent=%s action=%s: %v", p.TableName, p.ParentID, p.ActionType, err)
	}
}

func RecordActionPGX(ctx context.Context, pool *pgxpool.Pool, p ActionParams) {
	p.ParentID = strings.TrimSpace(p.ParentID)
	p.RequestedBy = strings.TrimSpace(p.RequestedBy)
	if p.ParentID == "" || p.RequestedBy == "" || pool == nil {
		return
	}
	attempts := []execAttempt{
		{
			query: fmt.Sprintf(`
			INSERT INTO %s (%s, actiontype, processing_status, reason, requested_by, requested_at, old_values, new_values, change_summary)
			VALUES ($1, $2, $3, $4, $5, now(), $6, $7, $8)
		`, p.TableName, p.ParentColumn),
			args: []interface{}{p.ParentID, p.ActionType, p.Status, NullIfBlank(p.Reason), p.RequestedBy, JSONValue(p.OldValues), JSONValue(p.NewValues), JSONValue(BuildChangeSummary(p.OldValues, p.NewValues))},
		},
		{
			query: fmt.Sprintf(`
			INSERT INTO %s (%s, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1, $2, $3, $4, $5, now())
		`, p.TableName, p.ParentColumn),
			args: []interface{}{p.ParentID, p.ActionType, p.Status, NullIfBlank(p.Reason), p.RequestedBy},
		},
	}
	if err := execFirstSuccess(ctx, pgxExecAdapter{pool: pool}, attempts); err != nil {
		logger.LogError("fx audit insert failed table=%s parent=%s action=%s: %v", p.TableName, p.ParentID, p.ActionType, err)
	}
}

func recordDecision(ctx context.Context, exec ExecContext, p DecisionParams) error {
	tableName := p.TableName
	parentColumn := p.ParentColumn
	parentID := p.ParentID
	status := p.Status
	checkerBy := p.CheckerBy
	comment := p.Comment
	attempts := []execAttempt{
		{
			query: fmt.Sprintf(`
			UPDATE %s
			SET processing_status = $1,
			    checker_by = $2,
			    checker_at = now(),
			    checker_comment = $3
			WHERE action_id = (
				SELECT action_id
				FROM %s
				WHERE %s = $4
				  AND processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL', 'pending')
				ORDER BY requested_at DESC, action_id DESC
				LIMIT 1
			)
		`, tableName, tableName, parentColumn),
			args: []interface{}{status, checkerBy, NullIfBlank(comment), parentID},
		},
		{
			query: fmt.Sprintf(`
			UPDATE %s
			SET processing_status = $1,
			    checker_by = $2,
			    checker_at = now(),
			    checker_comment = $3
			WHERE audit_id = (
				SELECT audit_id
				FROM %s
				WHERE %s = $4
				  AND processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL', 'pending')
				ORDER BY requested_at DESC, audit_id DESC
				LIMIT 1
			)
		`, tableName, tableName, parentColumn),
			args: []interface{}{status, checkerBy, NullIfBlank(comment), parentID},
		},
		{
			query: fmt.Sprintf(`
			UPDATE %s
			SET processing_status = $1,
			    checker_by = $2,
			    checker_at = now(),
			    checker_comment = $3
			WHERE %s = $4
			  AND processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL', 'pending')
		`, tableName, parentColumn),
			args: []interface{}{status, checkerBy, NullIfBlank(comment), parentID},
		},
	}
	return execFirstSuccess(ctx, exec, attempts)
}

func RecordDecision(ctx context.Context, db *sql.DB, p DecisionParams) {
	p.ParentID = strings.TrimSpace(p.ParentID)
	p.CheckerBy = strings.TrimSpace(p.CheckerBy)
	if p.ParentID == "" || p.CheckerBy == "" || db == nil {
		return
	}
	if err := recordDecision(ctx, sqlExecAdapter{db: db}, p); err != nil {
		logger.LogError("fx audit decision failed table=%s parent=%s status=%s: %v", p.TableName, p.ParentID, p.Status, err)
	}
}

func RecordDecisionPGX(ctx context.Context, pool *pgxpool.Pool, p DecisionParams) {
	p.ParentID = strings.TrimSpace(p.ParentID)
	p.CheckerBy = strings.TrimSpace(p.CheckerBy)
	if p.ParentID == "" || p.CheckerBy == "" || pool == nil {
		return
	}
	if err := recordDecision(ctx, pgxExecAdapter{pool: pool}, p); err != nil {
		logger.LogError("fx audit decision failed table=%s parent=%s status=%s: %v", p.TableName, p.ParentID, p.Status, err)
	}
}

func RecordDownload(ctx context.Context, db *sql.DB, p DownloadParams) {
	p.ParentID = strings.TrimSpace(p.ParentID)
	p.RequestedBy = strings.TrimSpace(p.RequestedBy)
	if p.ParentID == "" || p.RequestedBy == "" || db == nil {
		return
	}
	columns := []string{p.ParentColumn, "requested_by", "requested_at", "file_name", "upload_s3_key"}
	values := []string{"$1", "$2", "now()", "$3", "$4"}
	args := []interface{}{p.ParentID, p.RequestedBy, FileName(p.UploadS3Key), NullIfBlank(p.UploadS3Key)}
	next := 5
	for col, val := range p.ExtraColumns {
		if strings.TrimSpace(col) == "" {
			continue
		}
		columns = append(columns, col)
		values = append(values, fmt.Sprintf("$%d", next))
		args = append(args, strings.TrimSpace(val))
		next++
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", p.TableName, strings.Join(columns, ", "), strings.Join(values, ", "))
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		logger.LogError("fx audit download insert failed table=%s parent=%s: %v", p.TableName, p.ParentID, err)
	}
}

func RecordDownloadPGX(ctx context.Context, pool *pgxpool.Pool, p DownloadParams) {
	p.ParentID = strings.TrimSpace(p.ParentID)
	p.RequestedBy = strings.TrimSpace(p.RequestedBy)
	if p.ParentID == "" || p.RequestedBy == "" || pool == nil {
		return
	}
	columns := []string{p.ParentColumn, "requested_by", "requested_at", "file_name", "upload_s3_key"}
	values := []string{"$1", "$2", "now()", "$3", "$4"}
	args := []interface{}{p.ParentID, p.RequestedBy, FileName(p.UploadS3Key), NullIfBlank(p.UploadS3Key)}
	next := 5
	for col, val := range p.ExtraColumns {
		if strings.TrimSpace(col) == "" {
			continue
		}
		columns = append(columns, col)
		values = append(values, fmt.Sprintf("$%d", next))
		args = append(args, strings.TrimSpace(val))
		next++
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", p.TableName, strings.Join(columns, ", "), strings.Join(values, ", "))
	if _, err := pool.Exec(ctx, query, args...); err != nil {
		logger.LogError("fx audit download insert failed table=%s parent=%s: %v", p.TableName, p.ParentID, err)
	}
}

func BuildChangeSummary(oldValues, newValues interface{}) []map[string]interface{} {
	oldMap, okOld := oldValues.(map[string]interface{})
	newMap, okNew := newValues.(map[string]interface{})
	if !okOld || !okNew {
		return nil
	}
	changes := make([]map[string]interface{}, 0)
	for field, newValue := range newMap {
		oldValue := oldMap[field]
		if auditValuesEqual(oldValue, newValue) {
			continue
		}
		changes = append(changes, map[string]interface{}{
			"field":     field,
			"old_value": oldValue,
			"new_value": newValue,
		})
	}
	return changes
}

func auditValuesEqual(oldValue, newValue interface{}) bool {
	if isAuditBlankValue(oldValue) && isAuditBlankValue(newValue) {
		return true
	}
	if reflect.DeepEqual(oldValue, newValue) {
		return true
	}
	return fmt.Sprint(oldValue) == fmt.Sprint(newValue)
}

func isAuditBlankValue(value interface{}) bool {
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(v) == ""
	case []byte:
		return strings.TrimSpace(string(v)) == ""
	default:
		return false
	}
}

func Now() time.Time {
	return time.Now().UTC()
}
