package logger

import (
	"archive/zip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type LoggerService struct {
	Config        map[string]interface{}
	file          *os.File
	mu            sync.Mutex
	stopCh        chan struct{}
	wg            sync.WaitGroup
	currentLog    string
	maxFileBytes  int64
	retentionDays int
	folderPath    string
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
	l.file = file
	l.currentLog = logFile
	log.SetOutput(file)
	log.Println("[LoggerService] Started, writing to", logFile)

	// background goroutine for rotation and retention
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
		log.Println("[LoggerService] Stopping")
		return l.file.Close()
	}
	return nil
}

func (l *LoggerService) nextLogFileName() string {
	timestamp := time.Now().Format("20060102_150405")
	return filepath.Join(l.folderPath, fmt.Sprintf("app_%s.log", timestamp))
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
		// close current file who's size is now exceeded
		l.file.Close()
		// open new file
		newLog := l.nextLogFileName()
		file, err := os.OpenFile(newLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		l.file = file
		l.currentLog = newLog
		log.SetOutput(file)
		log.Println("[LoggerService] Rotated log file to", newLog)
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
		// add to zip
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
		// will remove old log file
		os.Remove(fullPath)
	}
}

func (l *LoggerService) LogAudit(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Printf("[AUDIT] %s", msg)
}

var GlobalLogger *LoggerService

func SetGlobalLogger(l *LoggerService) {
	GlobalLogger = l
}

// Example usage in any method:
// if logger.GlobalLogger != nil {
//     logger.GlobalLogger.LogAudit("ResourceManager started")
// }
