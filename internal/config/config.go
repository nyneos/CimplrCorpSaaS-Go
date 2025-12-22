package config

const (
	DefaultTimeZone       = "Asia/Kolkata"
	DefaultSchemeURL      = "https://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0"
	DefaultNavURL         = "https://www.amfiindia.com/spages/NAVAll.txt"
	DefaultSchemeSchedule = "* 9 * * *"
	BatchSize             = 1000

	// Sweep Configuration Constants
	DefaultSweepSchedule = "*/1 * * * *" // Run every minute to check cutoff times
	SweepBatchSize       = 100
)
