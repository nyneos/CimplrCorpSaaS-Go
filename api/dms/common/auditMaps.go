package common

import (
	"time"

	"github.com/jackc/pgx/v5"
)

// RowsToMaps converts a pgx result set into JSON-friendly maps (timestamps → RFC3339).
func RowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	fds := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fds))
		for i, fd := range fds {
			row[string(fd.Name)] = NormalizeAuditValue(vals[i])
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// NormalizeAuditValue formats time values for the FE audit mapper.
func NormalizeAuditValue(v interface{}) interface{} {
	switch t := v.(type) {
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	case *time.Time:
		if t == nil {
			return nil
		}
		return t.UTC().Format(time.RFC3339)
	default:
		return v
	}
}
