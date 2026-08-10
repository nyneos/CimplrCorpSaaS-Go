// Package dmsevent is a leaf facade for FireDmsEvent so handlers that sit under
// dmsjobs' dependency tree (e.g. payablerecievable via dashboardBuilder) can
// fire DMS events without an import cycle.
package dmsevent

import "github.com/jackc/pgx/v5/pgxpool"

// FireFunc matches dmsjobs.FireDmsEvent.
type FireFunc func(
	pool *pgxpool.Pool,
	moduleCode, subModuleCode, triggerType string,
	sourceIDs []string,
	triggeredBy string,
)

// Fire is set by dmsjobs init to dmsjobs.FireDmsEvent. Default is a silent no-op.
var Fire FireFunc = func(*pgxpool.Pool, string, string, string, []string, string) {
	// Intentional no-op: real implementation is wired in by dmsjobs.init via SetFire.
}

// SetFire registers the real FireDmsEvent implementation. Called once from dmsjobs.
func SetFire(fn FireFunc) {
	if fn != nil {
		Fire = fn
	}
}
