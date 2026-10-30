package bankstatement

import "time"

// Bounds for calls to the statement storage bucket.
const (
	storageTransferTimeout = 5 * time.Minute
	storageErrorBodyLimit  = 4 << 10
	storageDownloadLimit   = 200 << 20
)
