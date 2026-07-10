package config

import (
	"os"
	"strconv"
)

const (
	DefaultTimeZone       = "Asia/Kolkata"
	DefaultSchemeURL      = "https://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0"
	DefaultNavURL         = "https://www.amfiindia.com/spages/NAVAll.txt"
	DefaultSchemeSchedule = "0 9 * * *"
	BatchSize             = 1000

	// Sweep Configuration Constants
	DefaultSweepSchedule = "*/1 * * * *" // Run every minute to check cutoff times
	SweepBatchSize       = 100
	// DefaultSweepMaxRetries is the number of retries after the first failed attempt per initiation.
	DefaultSweepMaxRetries = 2
)

// SweepMaxRetries returns retries-after-first-failure from SWEEP_MAX_RETRIES (default 2).
// Total attempts per initiation = 1 + SweepMaxRetries().
func SweepMaxRetries() int {
	raw := os.Getenv("SWEEP_MAX_RETRIES")
	if raw == "" {
		return DefaultSweepMaxRetries
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return DefaultSweepMaxRetries
	}
	if n > 10 {
		return 10
	}
	return n
}

// SweepMaxAttemptsPerInitiation is initial attempt plus configured retries.
func SweepMaxAttemptsPerInitiation() int {
	return 1 + SweepMaxRetries()
}
