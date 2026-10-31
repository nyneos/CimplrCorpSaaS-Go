package bankstatement

import "time"

const (
	storageTransferTimeout = 5 * time.Minute
	storageErrorBodyLimit  = 4 << 10
	storageDownloadLimit   = 200 << 20
)
