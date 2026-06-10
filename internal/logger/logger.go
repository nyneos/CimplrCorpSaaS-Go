package logger

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type contextKey string

// TraceIDKey is used to store and retrieve a trace ID from context.
const TraceIDKey contextKey = "logger_trace_id"

// instance is the shared logrus logger used by all log functions.
var instance = logrus.New()

func init() {
	instance.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
		},
	})
	instance.SetLevel(logrus.InfoLevel)
	instance.SetOutput(os.Stdout)
}

// WithTraceID stores a trace ID in the context for context-aware log functions.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceIDKey, traceID)
}

// getLogContext extracts MODULE, SUBMODULE, and FILENAME from the caller's stack.
func getLogContext() (string, string, string) {
	// Skip 3 levels: runtime.Caller -> getLogContext -> LogXxx -> actual caller
	_, file, _, ok := runtime.Caller(3)
	if !ok {
		return "UNKNOWN", "UNKNOWN", "UNKNOWN"
	}

	fileName := filepath.Base(file)
	dir := filepath.Dir(file)
	parts := strings.Split(filepath.ToSlash(dir), "/")

	module := "CORE"
	submodule := "GENERAL"
	foundApi := false
	foundInternal := false

	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i] == "api" {
			foundApi = true
			if i+1 < len(parts) {
				module = strings.ToUpper(parts[i+1])
			}
			if i+2 < len(parts) {
				submodule = strings.ToUpper(parts[i+2])
			}
			break
		}
		if parts[i] == "internal" {
			foundInternal = true
			if i+1 < len(parts) {
				module = "INTERNAL"
				submodule = strings.ToUpper(parts[i+1])
			}
			break
		}
	}

	if !foundApi && !foundInternal {
		if len(parts) > 0 {
			module = strings.ToUpper(parts[len(parts)-1])
		}
	}

	return module, submodule, fileName
}

func baseFields(mod, sub, file string) logrus.Fields {
	return logrus.Fields{
		"module":    mod,
		"submodule": sub,
		"file":      file,
	}
}

func withTrace(ctx context.Context, fields logrus.Fields) logrus.Fields {
	if ctx != nil {
		if traceID, ok := ctx.Value(TraceIDKey).(string); ok && traceID != "" {
			fields["trace_id"] = traceID
		}
	}
	return fields
}

// LogInfo logs at INFO level with module/submodule/file context.
func LogInfo(msg string, args ...interface{}) {
	mod, sub, file := getLogContext()
	fullMsg := msg
	if len(args) > 0 {
		fullMsg = fmt.Sprintf(msg, args...)
	}
	instance.WithFields(baseFields(mod, sub, file)).Info(fullMsg)
}

// LogError logs at ERROR level with module/submodule/file context.
func LogError(msg string, args ...interface{}) {
	mod, sub, file := getLogContext()
	fullMsg := msg
	if len(args) > 0 {
		fullMsg = fmt.Sprintf(msg, args...)
	}
	instance.WithFields(baseFields(mod, sub, file)).Error(fullMsg)
}

// LogAudit logs an audit event at INFO level with event_type=audit.
func LogAudit(msg string, args ...interface{}) {
	mod, sub, file := getLogContext()
	fullMsg := msg
	if len(args) > 0 {
		fullMsg = fmt.Sprintf(msg, args...)
	}
	fields := baseFields(mod, sub, file)
	fields["event_type"] = "audit"
	instance.WithFields(fields).Info(fullMsg)
}

// LogInfoCtx logs at INFO level and includes trace_id from context if present.
func LogInfoCtx(ctx context.Context, msg string, args ...interface{}) {
	mod, sub, file := getLogContext()
	fullMsg := msg
	if len(args) > 0 {
		fullMsg = fmt.Sprintf(msg, args...)
	}
	instance.WithFields(withTrace(ctx, baseFields(mod, sub, file))).Info(fullMsg)
}

// LogErrorCtx logs at ERROR level and includes trace_id from context if present.
func LogErrorCtx(ctx context.Context, msg string, args ...interface{}) {
	mod, sub, file := getLogContext()
	fullMsg := msg
	if len(args) > 0 {
		fullMsg = fmt.Sprintf(msg, args...)
	}
	instance.WithFields(withTrace(ctx, baseFields(mod, sub, file))).Error(fullMsg)
}

// LogAuditCtx logs an audit event and includes trace_id from context if present.
func LogAuditCtx(ctx context.Context, msg string, args ...interface{}) {
	mod, sub, file := getLogContext()
	fullMsg := msg
	if len(args) > 0 {
		fullMsg = fmt.Sprintf(msg, args...)
	}
	fields := baseFields(mod, sub, file)
	fields["event_type"] = "audit"
	instance.WithFields(withTrace(ctx, fields)).Info(fullMsg)
}

type LoggerService struct {
	Config        map[string]interface{}
	file          *os.File
	dbFile        *os.File
	mu            sync.Mutex
	stopCh        chan struct{}
	wg            sync.WaitGroup
	currentLog    string
	currentDBLog  string
	maxFileBytes  int64
	retentionDays int
	folderPath    string
	dbLogger      *log.Logger
}

func NewLoggerService(config map[string]interface{}) *LoggerService {
	maxMB, _ := config["max_file_mb"].(int)
	if maxMB == 0 {
		if f, ok := config["max_file_mb"].(float64); ok {
			maxMB = int(f)
		}
	}
	retention, _ := config["retention_days"].(int)
	if retention == 0 {
		if f, ok := config["retention_days"].(float64); ok {
			retention = int(f)
		}
	}
	folder, _ := config["folder_path"].(string)
	if folder == "" {
		folder = "./logs"
	}
	return &LoggerService{
		Config:        config,
		stopCh:        make(chan struct{}),
		maxFileBytes:  int64(maxMB) * 1024 * 1024,
		retentionDays: retention,
		folderPath:    folder,
	}
}

func (l *LoggerService) Name() string {
	return "Logger"
}

func (l *LoggerService) Start() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := os.MkdirAll(l.folderPath, 0755); err != nil {
		return err
	}
	logFile := l.nextLogFileName()
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	dbLogFile := l.nextDBLogFileName()
	dbFile, err := os.OpenFile(dbLogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		_ = file.Close()
		return err
	}
	l.file = file
	l.dbFile = dbFile
	l.currentLog = logFile

	multiWriter := io.MultiWriter(file, os.Stdout)
	instance.SetOutput(multiWriter)
	instance.WithField("log_file", logFile).Info("[LoggerService] Started")

	l.wg.Add(1)
	go l.backgroundWorker()

	return nil
}

func (l *LoggerService) Stop() error {
	close(l.stopCh)
	l.wg.Wait()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		instance.Info("[LoggerService] Stopping")
		return l.file.Close()
	}
	return nil
}

func (l *LoggerService) nextLogFileName() string {
	timestamp := time.Now().Format("20060102_150405")
	return filepath.Join(l.folderPath, fmt.Sprintf("app_%s.log", timestamp))
}

func (l *LoggerService) nextDBLogFileName() string {
	timestamp := time.Now().Format("20060102_150405")
	return filepath.Join(l.folderPath, fmt.Sprintf("db_%s.log", timestamp))
}

func (l *LoggerService) rotateIfNeeded() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	info, err := l.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() >= l.maxFileBytes && l.maxFileBytes > 0 {
		l.file.Close()
		newLog := l.nextLogFileName()
		file, err := os.OpenFile(newLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		l.file = file
		l.currentLog = newLog

		multiWriter := io.MultiWriter(file, os.Stdout)
		instance.SetOutput(multiWriter)
		instance.WithField("log_file", newLog).Info("[LoggerService] Rotated log file")
	}
	return nil
}

func (l *LoggerService) backgroundWorker() {
	defer l.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	retentionTicker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	defer retentionTicker.Stop()

	for {
		select {
		case <-l.stopCh:
			return
		case <-ticker.C:
			l.rotateIfNeeded()
		case <-retentionTicker.C:
			l.zipAndCleanOldLogs()
		}
	}
}

func (l *LoggerService) zipAndCleanOldLogs() {
	if l.retentionDays <= 0 {
		return
	}
	cutoff := time.Now().AddDate(0, 0, -l.retentionDays)
	files, err := os.ReadDir(l.folderPath)
	if err != nil {
		return
	}
	zipName := filepath.Join(l.folderPath, fmt.Sprintf("logs_%s.zip", time.Now().Format("20060102")))
	zipFile, err := os.Create(zipName)
	if err != nil {
		return
	}
	defer zipFile.Close()
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	for _, f := range files {
		if f.IsDir() || filepath.Ext(f.Name()) != ".log" {
			continue
		}
		fullPath := filepath.Join(l.folderPath, f.Name())
		info, err := os.Stat(fullPath)
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		w, err := zipWriter.Create(f.Name())
		if err != nil {
			continue
		}
		src, err := os.Open(fullPath)
		if err != nil {
			continue
		}
		io.Copy(w, src)
		src.Close()
		os.Remove(fullPath)
	}
}

func (l *LoggerService) LogAudit(msg string) {
	LogAudit("%s", msg)
}

func (l *LoggerService) LogDBf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.dbLogger != nil {
		l.dbLogger.Printf(format, args...)
		return
	}
	log.Printf(format, args...)
}

var GlobalLogger *LoggerService

func SetGlobalLogger(l *LoggerService) {
	GlobalLogger = l
}
