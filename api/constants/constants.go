package constants

// Common error messages
const (
	ErrInvalidSession           = "invalid user_id or session"
	ErrInvalidJSON              = "invalid json or missing fields"
	ErrInvalidJSONRequired      = "invalid json or missing required fields"
	ErrInvalidJSONShort         = "Invalid JSON"
	ErrMissingUserID            = "Missing or invalid user_id in body"
	ErrUserIDRequired           = "user_id required"
	ErrDB                       = "DB error"
	ErrInvalidRequestBody       = "Invalid request body"
	ErrNoAccessibleBusinessUnit = "No accessible business units found"
	ErrFailedToQuery            = "Failed to query"
	ErrPleaseLogin              = "Please login to continue."
	ErrMethodNotAllowed         = "Method Not Allowed"
)

// DB / SQL error templates
const (
	ErrTxStartFailed  = "failed to start transaction: "
	ErrTxCommitFailed = "failed to commit transaction: "
	ErrCommitFailed   = "commit failed: "
	ErrQueryFailed    = "query failed: "
	FormatSQLError    = "ERROR: %s"
)

// SQL formatting patterns
const (
	FormatSQLSetPair     = "%s=$%d, %s=$%d"
	FormatSQLColumnArg   = "%s = $%d"
	FormatPipelineTriple = "%s|%s|%s"
	FormatInsertAuditLog = "('%s','CREATE','PENDING_APPROVAL',NULL,'%s',now())"
)

// Content Types
const (
	ContentTypeJSON = "application/json"
	ContentTypeText = "Content-Type"
)

// Date formats
const (
	DateTimeFormat = "2006-01-02 15:04:05"
	DateFormat     = "2006-01-02"
	DateFormatAlt  = "02-01-2006"
	DateFormatISO  = "2006-01-02T15:04:05"
)
