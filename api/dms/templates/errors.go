package templates

import "errors"

var (
	errNoVersionOnAudit  = errors.New("pending audit row is missing its version_id")
	errUnknownActionType = errors.New("unknown pending audit action_type")
)
