package constants

// Common error messages
const (
	// ErrInvalidSession                = "invalid user_id or session" //"User not found in active sessions".  "Invalid or inactive session"
	ErrInvalidJSON                   = "invalid json or missing fields"
	ErrInvalidJSONRequired           = "invalid json or missing required fields"
	ErrExposureHeaderIDsUserID       = "exposureHeaderIds and user_id are required"
	ErrInvalidJSONBOM                = "\uFEFF"
	ErrExposureUploadFilenamePattern = "upload-*.xlsx"
	ErrInvalidJSONShort              = "Invalid JSON"
	ErrInvalidJSONPrefix             = "invalid JSON: "
	// ErrMissingUserID                 = "Missing or invalid user_id in body"
	ErrUserIDRequired     = "user_id required"
	ErrDB                 = "DB error"
	ErrDBPrefix           = "DB error: "
	ErrUpdateFailed       = "update failed: "
	ErrPreviewBuild       = "preview build: "
	ErrFailedToParseForm  = "Failed to parse form: "
	ErrAuditInsertFailed  = "Audit insert failed: "
	ErrTxBeginFailed      = "TX begin failed: "
	ErrInvalidRequestBody = "Invalid request body"
	// ErrNoAccessibleBusinessUnit      = "No accessible business units found"
	ErrFailedToQuery             = "Failed to query"
	ErrPleaseLogin               = "Please login to continue."
	ErrScanFailed                = "scan failed"
	ErrScanFailedPrefix          = "scan failed: "
	ErrMethodNotAllowed          = "Method Not Allowed"
	ErrUserIIsRequired           = "user_id is required"
	ErrRequiredColumnNotFound    = "required column '%s' not found in header"
	ErrIndexRequired             = "index required"
	ErrReferenceRequired         = "your reference is required"
	ErrDebitCreditReference      = "debit/credit reference"
	ErrDebitCreditReferenceAlt   = "debit credit reference"
	ErrDebitCreditReferenceShort = "debit credit ref"
	ErrDebitCreditReference2     = "debit/credit ref"
)

// General internal/server/upload error messages
const (
	// ErrInternalServer                 = "Internal server error"
	ErrFailedToParseMultipartForm     = "Failed to parse multipart form"
	ErrNoFilesUploaded                = "No files uploaded"
	ErrFailedToOpenFile               = "Failed to open uploaded file"
	ErrFailedToReadCSVHeaders         = "Failed to read CSV headers"
	ErrFailedToParseExcelFile         = "Failed to parse Excel file"
	ErrFailedToCopyFile               = "Failed to copy file"
	ErrFailedToCreateTempFile         = "Failed to create temp file"
	ErrNoDataToUpload                 = "No data to upload"
	ErrInvalidOrEmptyFile             = "Invalid or empty file"
	ErrFailedToStageData              = "Failed to stage data"
	ErrFailedToReadInsertedRows       = "Failed to read inserted rows"
	ErrFailedToInsertAuditActions     = "Failed to insert audit actions"
	ErrFailedToInsertClearingCodes    = "Failed to insert clearing codes"
	ErrFailedToDeleteOldClearingCodes = "Failed to delete old clearing codes"
	ErrFailedToFetchExistingAccount   = "Failed to fetch existing account"
	ErrInvalidClearingCodesPayload    = "Invalid clearing_codes payload"
	ErrUnsupportedFileType            = "unsupported file type"
	ErrNoMappingForSourceColumn       = "No mapping for source column: %s"
	ErrActivityInsertFailed           = "Activity insert failed: "
	ErrMTMInsertFailed                = "MTM insert failed: "
	ErrSettingKeyRequired             = "setting_key parameter is required"
	ErrSettingNotFound                = "Setting not found"
	ErrBankStatementAlreadyExists     = "Bank Statement Transactions exist in system delete them first"
	ErrTransactionHeaderRowNotFound   = "transaction header row not found in Excel file"
)

// Additional common messages used across handlers
const (
	ErrInvalidSessionShort            = "Invalid session"
	ErrUnauthorizedEntity             = "unauthorized entity"
	ErrInvalidCSV                     = "Invalid CSV file"
	ErrNoAccessibleEntitiesForRequest = "No accessible entities found for request"
	ErrUnauthorizedFolioIDsFormat     = "unauthorized folio_ids: %v"
	// ErrInvalidCurrency                = "Invalid or unsupported currency"
	// ErrInvalidBank                    = "Invalid or unsupported bank"
	// ErrInvalidCashFlowCategory        = "Invalid or unsupported cash flow category"
	ErrInvalidAMC     = "Invalid or inactive AMC"
	ErrInvalidAccount = "Invalid or inactive account"
)

// Additional user-facing messages for folio handlers
const (
	ErrInvalidFormDataUser          = "Invalid form data"
	ErrNoFileUploaded               = "no file uploaded"
	ErrNoRowsProvided               = "No rows provided"
	ErrMissingRequiredFieldsUser    = "Missing required fields"
	ErrFolioAlreadyExistsUser       = "Folio already exists"
	ErrTxStartFailedUser            = "Unable to start database transaction"
	ErrInsertFailedUser             = "Insert failed"
	ErrFolioSchemeMappingFailedUser = "Failed to map schemes"
	ErrAuditInsertFailedUser        = "Failed to create audit entry"
	ErrCommitFailedUser             = "Commit failed"
	ErrFolioIDRequiredUser          = "folio_id required"
	ErrNoFieldsToUpdateUser         = "no fields to update"
	ErrAMCValidationFailedUser      = "AMC validation failed: "
	ErrCalendarIDRequiredUser       = "calendar_id required"
	ErrFolioIDMissingUser           = "folio_id missing"
	ErrFetchFailedUser              = "fetch failed"
	ErrValidateFolioIDsUser         = "Failed to validate folio ids"
	ErrNoApprovableActions          = "No approvable actions found"
	ErrNoAuditActionFoundFormat     = "no audit action found for folio_ids: %v. "
	ErrCannotRejectApprovedFormat   = "cannot reject already approved folio_ids: %v"
	ErrDBConnection                 = "internal server error: db connection"
)

// DB / SQL error templates
const (
	// ErrTxStartFailed  = "failed to start transaction: "
	ErrTxStartFailed  = "Failed to start transaction,"
	ErrTxCommitFailed = "failed to commit transaction: "
	ErrCommitFailed   = "commit failed: "
	// ErrQueryFailed         = "query failed: "
	FormatSQLError         = "ERROR: %s"
	ErrRowsError           = "rows error: "
	ErrRowsScanFailed      = "rows scan failed: "
	ErrUnsupportedProvider = "unsupported provider"
)

// SQL formatting patterns
const (
	FormatSQLSetPair        = "%s=$%d, %s=$%d"
	FormatSQLColumnArg      = "%s = $%d"
	FormatPipelineTriple    = "%s|%s|%s"
	FormatPipelineTripleAlt = "%s||%s||%s"
	FormatInsertAuditLog    = "('%s','CREATE','PENDING_APPROVAL',NULL,'%s',now())"
	FormatFiscalYear        = "FY %d-%d"
)

// Content Types
const (
	ContentTypeJSON        = "application/json"
	ContentTypeText        = "Content-Type"
	ContentTypeMultipart   = "multipart/form-data"
	ErrFailedToPrepareFile = "Failed to prepare file for parsing"
)

// Generic keys and small common tokens
const (
	ValueSuccess                 = "success"
	ErrDBAcquire                 = "db acquire: "
	ErrTxBegin                   = "tx begin: "
	LogWarn                      = "[WARN] %s"
	ValueError                   = "error"
	KeyStatus                    = "status"
	KeyUserID                    = "user_id"
	ErrInvalidSessionCapitalized = "Invalid user_id or session"
	ErrCommitFailedCapitalized   = "Commit failed: "
	ErrTxBeginFailedCapitalized  = "tx begin failed: "
	ErrBeginTransactionFailed    = "begin transaction failed: %w"
	ErrDuplicateKey              = "duplicate key"
)

// Date formats
const (
	DateTimeFormat      = "2006-01-02 15:04:05"
	DateFormat          = "2006-01-02"
	DateFormatAlt       = "02-01-2006"
	DateFormatISO       = "2006-01-02T15:04:05"
	DateFormatSlash     = "02/Jan/2006"
	DateFormatDash      = "02-Jan-2006"
	DateFormatCustom    = "01-01-1"
	DateFormatYearMonth = "2006-01"
)

const (
	ErrPlanIDRequired                   = "plan_id is required"
	ErrFailedToGetGroups                = "failed to get groups: "
	ErrFailedToScanGroupID              = "failed to scan group ID: "
	ErrPrefixPayable                    = "TR-PAY-"
	ErrPrefixReceivable                 = "TR-REC-"
	FormatTransactionID                 = "%s-%d-%d"
	ErrFailedToInsertMonthlyProjections = "Failed to insert monthly projections: "
	ErrFailedToBeginTransaction         = "failed to begin transaction"
	ErrEntityNameRequired               = "entity_name is required"
	ErrProposalIDsRequired              = "proposal_ids are required"
	ErrAutoApprovedEnrichedEntity       = "Auto-approved enriched entity"
	ErrBulkFolioMappingInsertFailed     = "[bulk] folio mapping insert failed: %v"
	ErrBulkSchemeMappingInsertFailed    = "[bulk] scheme mapping insert failed: %v"
	ErrBulkFolioMappingFailed           = "folio mapping failed: "
	ErrBulkSchemeMappingFailed          = "scheme mapping failed: "
	ErrBulkOnboardMappingFailed         = "onboard mapping failed: "
	ErrBulkRelationshipInsertFailed     = "relationship insert failed: "
	// ErrEntityNotFound                   = "Entity not found"
	ErrAuthServiceUnavailable             = "Auth service unavailable"
	ErrNoRowsUpdated                      = "No rows updated"
	ErrFailedToFetchCategoryRelationships = "Failed to fetch category relationships"
	ErrAlreadyExists                      = "already exists"
	ErrAMCNotFoundOrNotApprovedActive     = "AMC not found or not approved/active: "
	ErrUnableToUpdateParentAccountBalance = "Unable to update parent account balance"
	ErrUnableToLogSweepExecution          = "Unable to log sweep execution"
	ErrClosingBalance                     = "closing balance"
)
const (
	StatusCodeAwaitingApproval = "Awaiting-Approval"
	StatusCodeDeleteApproval   = "Delete-Approval"
)

const (
	HeaderAccessControlAllowOrigin       = "Access-Control-Allow-Origin"
	HeaderAccessControlAllowMethods      = "Access-Control-Allow-Methods"
	HeaderAccessControlAllowMethodsValue = "GET, POST, PUT, DELETE, OPTIONS"
	HeaderAccessControlAllowHeaders      = "Access-Control-Allow-Headers"
	HeaderAccessControlAllowHeadersValue = "Content-Type, Authorization"
)

const (
	QuerryCounterpartyName         = "COALESCE(m.counterparty_name, 'Generic')"
	QuerryGeneric                  = "'Generic'"
	VendorPayment                  = "'Vendor Payment'"
	ExposureBucketing              = "exposure-bucketing"
	ExposureUpload                 = "exposure-upload"
	QuerryEntity                   = " AND eh.entity = ANY($%d)"
	QuerryHighUnhedgedExposure     = "High Unhedged Exposure"
	QuerryWhereClause              = "WHERE eh.entity = ANY($2) AND"
	QuerryCurrency2                = " AND eh.currency = ANY($%d)"
	QuerryEntityName               = " AND cpi.entity_name = $%d"
	QuerryCurrencyCode             = " AND cp.currency_code = $%d"
	QuerryFplCurrency              = " AND fpl.currency = $%d"
	QuerryFilterGroup              = " AND fg.primary_key = 'entity_name' AND fg.primary_value = $%d"
	QuerryBankName                 = "(b.bank_name IS NULL OR b.bank_name = ANY($%d))"
	QuerryCurrency                 = "(a.currency IS NULL OR a.currency = ANY($%d))"
	QuerryEntityNameLower          = " AND lower(trim(c.entity_name)) = ANY($%d)"
	QuerryBankNameLower            = " AND lower(trim(c.bank_name)) = ANY($%d)"
	FormatInvestmentID             = "INV-%s"
	FormatInvestmentName           = "Investment - %s"
	FormatMTMGain                  = "MTM gain - %s"
	FormatMTMLoss                  = "MTM loss - %s"
	FormatLogMore                  = "  ... (%d more) ...\n"
	QuerryBusinessUnitName         = "SELECT business_unit_name FROM users WHERE id = $1"
	StorageObjectURLFormat         = "%s/storage/v1/object/%s/%s"
	QuerrySweepID                  = " AND l.sweep_id = $%d"
	BearerPrefix                   = "Bearer "
	NBSP                           = "\u00A0"
	TransactionPostedDate          = "txn posted date"
	TransactionPostingDate         = "posting date"
	TransactionPostedDateAlt       = "posted date"
	TransactionDate                = "transaction date"
	ValueDate                      = "value date"
	CreditAmount                   = "credit amt"
	DebitAmount                    = "debit amt"
	ClosingBalance                 = "closing balance"
	BalanceCarriedForward          = "balance carried forward"
	ErrFailedToInsertBankStatement = "failed to insert bank statement: %w"
	ErrNoAccessToBankStatement     = "No access to this bank statement"
	ErrFailedToInsertAuditAction   = "failed to insert audit action: %w"
)

var (
	Nifty50 = "NIFTY 50"
)
