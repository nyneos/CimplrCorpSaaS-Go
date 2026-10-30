package bankstatement

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
)

const (
	maxZipEntries          = 200
	maxZipEntryBytes int64 = 50 << 20
	maxZipTotalBytes int64 = 200 << 20
)

var errZipLimitExceeded = errors.New("zip archive exceeds the allowed size or number of files")

// zipReadBudget enforces decompressed-size limits on the bytes actually read from an
// archive, not on the sizes declared in its headers, so a small zip cannot expand
// into gigabytes of memory.
type zipReadBudget struct {
	remaining int64
}

func newZipReadBudget() *zipReadBudget {
	return &zipReadBudget{remaining: maxZipTotalBytes}
}

func checkZipEntryCount(zr *zip.Reader) error {
	if len(zr.File) > maxZipEntries {
		return fmt.Errorf("%w: more than %d files", errZipLimitExceeded, maxZipEntries)
	}
	return nil
}

func (b *zipReadBudget) read(f *zip.File) ([]byte, error) {
	limit := maxZipEntryBytes
	if b.remaining < limit {
		limit = b.remaining
	}
	rc, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%w: %s is too large", errZipLimitExceeded, f.Name)
	}
	b.remaining -= int64(len(data))
	return data, nil
}
