package pollcursor

import "strings"

// SkipSet is a set of provider message IDs already ingested (skip MIME/S3/parse).
type SkipSet map[string]struct{}

func NewSkipSet(ids []string) SkipSet {
	out := make(SkipSet, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			out[id] = struct{}{}
		}
	}
	return out
}

func (s SkipSet) Has(id string) bool {
	if s == nil {
		return false
	}
	_, ok := s[strings.TrimSpace(id)]
	return ok
}
