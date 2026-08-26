package logger

import (
	"context"
	"io"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"
)

// dbInstance is the dedicated JSON logger for DB query records. It writes only
// to the db_*.log file managed by LoggerService — never to stdout — so the
// file stays pure, machine-parseable JSON, one query per line.
var dbInstance = logrus.New()

func init() {
	dbInstance.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
		},
	})
	dbInstance.SetLevel(logrus.InfoLevel)
	// Until LoggerService.Start() calls SetDBOutput with the real file, hold
	// query records here instead of losing them or spraying raw JSON to stdout.
	dbInstance.SetOutput(io.Discard)
}

// SetDBOutput points the DB query logger at the active db_*.log file. Called by
// LoggerService on start and on rotation.
func SetDBOutput(w io.Writer) {
	dbInstance.SetOutput(w)
}

type dbTraceCtxKey struct{}

type dbQueryTrace struct {
	sql   string
	args  []interface{}
	start time.Time
}

// DBTracer implements pgx.QueryTracer and pgx.BatchTracer. Attach one to a
// pgxpool.Config's ConnConfig.Tracer field to log every query pgx runs on that
// pool: exact SQL text, bound args, row count, duration, and success/failure —
// structured JSON to db_*.log, plus a colorized one-line summary to stdout.
type DBTracer struct {
	// Service labels which pool/module issued the query (e.g. "core", "fx",
	// "master", "cash", "dash", "uam") since several modules run their own
	// separate pgx pool.
	Service string
}

func NewDBTracer(service string) *DBTracer {
	return &DBTracer{Service: service}
}

func (t *DBTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, dbTraceCtxKey{}, &dbQueryTrace{sql: data.SQL, args: data.Args, start: time.Now()})
}

func (t *DBTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	tr, _ := ctx.Value(dbTraceCtxKey{}).(*dbQueryTrace)
	if tr == nil {
		return
	}
	t.record(ctx, tr.sql, tr.args, time.Since(tr.start), data.CommandTag.String(), data.CommandTag.RowsAffected(), data.Err)
}

func (t *DBTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return context.WithValue(ctx, dbTraceCtxKey{}, &dbQueryTrace{start: time.Now()})
}

func (t *DBTracer) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchQueryData) {
	tr, _ := ctx.Value(dbTraceCtxKey{}).(*dbQueryTrace)
	dur := time.Duration(0)
	if tr != nil {
		dur = time.Since(tr.start)
	}
	t.record(ctx, data.SQL, data.Args, dur, data.CommandTag.String(), data.CommandTag.RowsAffected(), data.Err)
}

func (t *DBTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	if data.Err != nil {
		t.record(ctx, "(batch end)", nil, 0, "", 0, data.Err)
	}
}

func (t *DBTracer) record(ctx context.Context, sql string, args []interface{}, dur time.Duration, commandTag string, rows int64, queryErr error) {
	fields := logrus.Fields{
		"event_type":  "db_query",
		"service":     t.Service,
		"sql":         sql,
		"args":        args,
		"args_count":  len(args),
		"rows":        rows,
		"command_tag": commandTag,
		"duration_ms": dur.Milliseconds(),
	}
	if traceID, ok := ctx.Value(TraceIDKey).(string); ok && traceID != "" {
		fields["trace_id"] = traceID
	}

	oneLineSQL := strings.Join(strings.Fields(sql), " ")

	if queryErr != nil {
		fields["status"] = "FAILURE"
		fields["error"] = queryErr.Error()
		dbInstance.WithFields(fields).Error("db query")
		color.New(color.FgRed, color.Bold).Fprint(os.Stdout, "[DB FAIL] ")
		color.New(color.FgRed).Fprintf(os.Stdout, "%-8s %6dms  %s -- %v\n", t.Service, dur.Milliseconds(), oneLineSQL, queryErr)
		return
	}

	fields["status"] = "SUCCESS"
	dbInstance.WithFields(fields).Info("db query")
	color.New(color.FgGreen, color.Bold).Fprint(os.Stdout, "[DB OK]   ")
	color.New(color.FgGreen).Fprintf(os.Stdout, "%-8s %6dms  rows=%-4d  %s\n", t.Service, dur.Milliseconds(), rows, oneLineSQL)
}
