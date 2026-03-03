package jobs

// import (
//     "database/sql"
// )

// SweepExecParams groups sweep execution inputs to avoid long parameter lists.
type SweepExecParams struct {
	SweepID          string
	InitiationID     string
	FromAccount      string
	ToAccount        string
	SweepType        string
	BufferAmount     *float64
	SweepAmount      *float64
	OverriddenAmount *float64
	RequestedBy      string
}

// SweepFailureInfo groups fields used when logging a sweep failure.
type SweepFailureInfo struct {
	Params        SweepExecParams
	BalanceBefore float64
	Reason        string
}
