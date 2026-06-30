package constants

// AppCode bundles the custom app status code (sent in the JSON body) with its
// corresponding HTTP status code (written to the wire) and a default message.
type AppCode struct {
	Code     int    // custom app code, e.g. 4003
	HTTPCode int    // HTTP status for the wire, e.g. 401
	Message  string // default message if caller doesn't override
}

var (
	// 2xxx — Success
	StatusSuccess = AppCode{2000, 200, "Success"}
	StatusCreated = AppCode{2001, 201, "Resource created successfully"}
	StatusUpdated = AppCode{2002, 200, "Resource updated successfully"}
	StatusDeleted = AppCode{2003, 200, "Resource deleted successfully"}

	// 4xxx — Client errors
	StatusBadRequest     = AppCode{4000, 400, "Bad request"}
	StatusValidation     = AppCode{4001, 422, "Validation failed"}
	StatusUnauthorized   = AppCode{4002, 401, "Unauthorized"}
	StatusSessionExpired = AppCode{4003, 401, "Session expired. Please login again"}
	StatusForbidden      = AppCode{4004, 403, "Access denied"}
	StatusNotFound       = AppCode{4005, 404, "Resource not found"}
	StatusDuplicate      = AppCode{4006, 400, "Duplicate entry"}
	StatusLimitExceeded  = AppCode{4007, 422, "Limit exceeded"}
	StatusPayloadTooLarge = AppCode{4008, 413, "Request payload too large"}

	// 5xxx — Server errors
	StatusInternalError = AppCode{5000, 500, "Something went wrong"}
	StatusDBError       = AppCode{5001, 500, "Something went wrong"}
	StatusExternalError = AppCode{5002, 502, "External service unavailable"}
)
