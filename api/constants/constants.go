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
	ErrStatementNotFound  = "statement not found"
	ErrUserIDRequired     = "user_id required"
	ErrFileIDRequired     = "file_id required"
	ErrFileNotFound       = "file not found"
	ErrDB                 = "DB error"
	ErrDBPrefix           = "DB error: "
	ErrUpdateFailed       = "update failed: "
	ErrPreviewBuild       = "preview build: "
	ErrFailedToParseForm  = "Failed to parse form: "
	ErrAuditInsertFailed  = "Audit insert failed: "
	ErrTxBeginFailed      = "TX begin failed: "
	ErrInvalidRequestBody = "Invalid request body"
	ErrFDBookingPanic     = "[FDBooking] notification goroutine panic for booking %s: %v"
	// ErrNoAccessibleBusinessUnit      = "No accessible business units found"
	ErrFailedToQuery                 = "Failed to query"
	ErrPleaseLogin                   = "Please login to continue."
	ErrScanFailed                    = "scan failed"
	ErrScanFailedPrefix              = "scan failed: "
	ErrMethodNotAllowed              = "Method Not Allowed"
	ErrUserIIsRequired               = "user_id is required"
	ErrRequiredColumnNotFound        = "required column '%s' not found in header"
	ErrIndexRequired                 = "index required"
	ErrReferenceRequired             = "your reference is required"
	ErrDebitCreditReference          = "debit/credit reference"
	ErrDebitCreditReferenceAlt       = "debit credit reference"
	ErrDebitCreditReferenceShort     = "debit credit ref"
	ErrDebitCreditReference2         = "debit/credit ref"
	ErrStrategyRequired              = "__STRATEGY__:"
	ErrAIBulk                        = "[AI-BULK] %s"
	ErrPendingRolloverNotUpdated     = "pending rollover was not updated"
	ErrPendingCancellationNotUpdated = "pending cancellation was not updated"
	ErrInsertFailed                  = "INSERT INTO %s (%s) VALUES (%s)"
	ErrDateInterval                  = "'::date + INTERVAL '1 day'"
	ErrParentLookupFailed            = "parent lookup failed: "
	ErruploadMetaFail                = "file upload metadata failed: "
	ErrFailedToResolveStagingFiles   = "failed to resolve staging files"
)

// General internal/server/upload error messages
const (
	// ErrInternalServer                 = "Internal server error"
	ErrFailedToParseMultipartForm          = "Failed to parse multipart form"
	ErrNoFilesUploaded                     = "No files uploaded"
	ErrFailedToOpenFile                    = "Failed to open uploaded file"
	ErrFailedToReadFile                    = "Failed to read file: "
	ErrFailedToStoreFile                   = "Failed to store file: "
	ErrFailedToReadCSVHeaders              = "Failed to read CSV headers"
	ErrFailedToParseExcelFile              = "Failed to parse Excel file"
	ErrFailedToCopyFile                    = "Failed to copy file"
	ErrFailedToCreateTempFile              = "Failed to create temp file"
	ErrNoDataToUpload                      = "No data to upload"
	ErrInvalidOrEmptyFile                  = "Invalid or empty file"
	ErrFailedToStageData                   = "Failed to stage data"
	ErrFailedToReadInsertedRows            = "Failed to read inserted rows"
	ErrFailedToValidateDuplicateInitiation = "failed to validate duplicate initiation: "
	ErrDuplicateInitiationExists           = "duplicate initiation/config exists for the same entity+accounts+time; cannot create initiation"
	ErrFailedToInsertAuditActions          = "Failed to insert audit actions"
	ErrFailedToInsertClearingCodes         = "Failed to insert clearing codes"
	ErrFailedToDeleteOldClearingCodes      = "Failed to delete old clearing codes"
	ErrFailedToFetchExistingAccount        = "Failed to fetch existing account"
	ErrInvalidClearingCodesPayload         = "Invalid clearing_codes payload"
	ErrUnsupportedFileType                 = "unsupported file type"
	ErrNoMappingForSourceColumn            = "No mapping for source column: %s"
	ErrActivityInsertFailed                = "Activity insert failed: "
	ErrMTMInsertFailed                     = "MTM insert failed: "
	ErrSettingKeyRequired                  = "setting_key parameter is required"
	ErrSettingNotFound                     = "Setting not found"
	ErrBankStatementAlreadyExists          = "Bank Statement Transactions exist in system delete them first"
	ErrTransactionHeaderRowNotFound        = "transaction header row not found in Excel file"
	ErrInterestTypeCodeAlreadyExists       = "Interest type code already exists and is active."
	ErrInterestTypeNameAlreadyExists       = "Interest type name already exists and is active."
	ErrNoInterestIDsProvided               = "No interest IDs provided"
	ErrNotdsIDsProvided                    = "No tds plan IDs provided"
	ErrEventIDsRequired                    = "event_ids required"
	ErrTemplateIDRequired                  = "template_id required"
	ErrAuditIDsRequired                    = "audit_ids required"
	ErrFailedToStartDBTransaction          = "Failed to start DB transaction"
	ErrRunIDRequired                       = "run_id is required"
	ErrRunIDAndFDIDRequired                = "run_id and fd_id are required"
	ErrConfigIDRequired                    = "config_id is required"
	ErrScheduleConfigNotFound              = "schedule config not found"
	ErrLoadBookingSchemaFailed             = "Load booking schema failed"
	ErrBookingIDsRequired                  = "booking_ids are required"
	ErrConfirmationIDsRequired             = "confirmation_ids are required"
	ErrFDIDRequired                        = "fd_id is required"
	ErrCashflowIDsRequired                 = "cashflow_ids are required"
	ErrCashflowTableNotFound               = "cashflow table not found"
	ErrAuditIDRequired                     = "audit_id is required"
	ErrAuditRecordNotFound                 = "audit record not found"
	ErrMakerCheckerSamePerson              = "maker and checker cannot be the same person"
	ErrReceiptIDRequired                   = "receipt_id is required"
	ErrClosureRequestIDRequired            = "closure_request_id is required"
	ErrClosureRequestNotFound              = "closure request not found"
	ErrDayCountCodeRequired                = "day_count_code is required"
	ErrClosureInitiateIDRequired           = "closure_initiate_id is required"
	ErrClosureInitiateRecordNotFound       = "closure initiate record not found"
	ErrFailedToReadBankBalanceAuditHistory = "failed to read bank balance download audit history"
	ErrMissingUserIDOrBankStatementID      = "Missing user_id or bank_statement_id"
	// ErrFailedToDeleteStagedStatements              = "failed to delete staged statements"
	ErrFailedToReadTransactionDownloadAuditHistory = "failed to read transaction download audit history"
	ErrMissingLatestAuditForTransaction            = "missing latest audit for transaction: "
	ErrBankStatementIDRequired                     = "bank_statement_id is required"
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
	ErrInvalidFormDataUser                = "Invalid form data"
	ErrNoFileUploaded                     = "no file uploaded"
	ErrNoRowsProvided                     = "No rows provided"
	ErrMissingRequiredFieldsUser          = "Missing required fields"
	ErrFolioAlreadyExistsUser             = "Folio already exists"
	ErrTxStartFailedUser                  = "Unable to start database transaction"
	ErrInsertFailedUser                   = "Insert failed"
	ErrFolioSchemeMappingFailedUser       = "Failed to map schemes"
	ErrAuditInsertFailedUser              = "Failed to create audit entry"
	ErrCommitFailedUser                   = "Commit failed"
	ErrFolioIDRequiredUser                = "folio_id required"
	ErrNoFieldsToUpdateUser               = "no fields to update"
	ErrAMCValidationFailedUser            = "AMC validation failed: "
	ErrCalendarIDRequiredUser             = "calendar_id required"
	ErrFolioIDMissingUser                 = "folio_id missing"
	ErrFetchFailedUser                    = "fetch failed"
	ErrValidateFolioIDsUser               = "Failed to validate folio ids"
	ErrNoApprovableActions                = "No approvable actions found"
	ErrNoAuditActionFoundFormat           = "no audit action found for folio_ids: %v. "
	ErrCannotRejectApprovedFormat         = "cannot reject already approved folio_ids: %v"
	ErrDBConnection                       = "internal server error: db connection"
	ErrNoFrequencyIDsProvided             = "No frequency IDs provided"
	ErrEventIDChannelTemplateNameRequired = "event_id, channel and template_name are required"
	ErrFailedToValidateUniqueness         = "failed to validate uniqueness: "
	ErrCompoundingPeriodsPerYear          = "compounding_periods_per_year must be >= 1"
	ErrMasterCounterparty                 = "master-counterparty"
	ErrMasterEntityCash                   = "master-entity-cash"
	ErrMasterGLAccount                    = "master-gl-account"
)

// Master unique violation messages (PostgreSQL 23505).
const (
	ErrAMCNameAlreadyExists          = "AMC name already exists. Please use a different name."
	ErrInternalAMCCodeAlreadyExists  = "Internal AMC code already exists. Please use a different code."
	ErrSEBIRegistrationAlreadyExists = "SEBI registration number already exists."
	ErrEntityNameAlreadyExists       = "Entity name already exists. Please use a different name."
)

// DB / SQL error templates
const (
	// ErrTxStartFailed  = "failed to start transaction: "
	ErrTxStartFailed  = "Failed to start transaction,"
	ErrTxCommitFailed = "failed to commit transaction: "
	ErrCommitFailed   = "commit failed: "
	// ErrQueryFailed         = "query failed: "
	ErrExceptionNotFound   = "Exception not found"
	FormatSQLError         = "ERROR: %s"
	ErrRowsError           = "rows error: "
	ErrRowError            = "Row error: "
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
	FormatYearWeek          = "%d-W%02d"
	FormatYearQuarter       = "%d-Q%d"
	FormatYearMonth         = "%d-%02d"
	FormatTuple             = "($%d,$%d)"
	FormatSQLColumnArgAlt   = "%s=$%d"
)

// Content Types
const (
	ContentTypeJSON        = "application/json"
	ContentTypeText        = "Content-Type"
	ContentTypeMultipart   = "multipart/form-data"
	ErrFailedToPrepareFile = "Failed to prepare file for parsing"
	ContentTypeJSONUTF8    = "application/json; charset=utf-8"
)

// Generic keys and small common tokens
const (
	ValueSuccess                  = "success"
	ErrDBAcquire                  = "db acquire: "
	ErrTxBegin                    = "tx begin: "
	LogWarn                       = "[WARN] %s"
	LogSweepWorker                = "[SWEEP V2] ═══════════════════════════════════════════════════════"
	ValueError                    = "error"
	KeyStatus                     = "status"
	KeyUserID                     = "user_id"
	ErrInvalidSessionCapitalized  = "Invalid user_id or session"
	ErrCommitFailedCapitalized    = "Commit failed: "
	ErrTxBeginFailedCapitalized   = "tx begin failed: "
	ErrBeginTransactionFailed     = "begin transaction failed: %w"
	ErrDuplicateKey               = "duplicate key"
	ErrEmptyString                = "''::text"
	ErrBankIDRequired             = "bank_id is required"
	ErrBankIDFilter               = "mba.bank_id = $%d"
	ErrWhereClause                = "WHERE "
	FormatDeletedFilter           = "AND COALESCE(p.is_deleted, FALSE) = FALSE"
	FormatCOALESCE                = "COALESCE(%s.%s,'')"
	ErrTDSIDRequired              = "tds_id is required"
	ErrEntityNameFilter           = " AND cpi.entity_name = ANY($%d)"
	ErrCurrencyCodeFilter         = " AND UPPER(TRIM(COALESCE(cp.currency_code, ''))) = ANY($%d)"
	ErrCurrencyFilter             = " AND UPPER(TRIM(fpl.currency)) = ANY($1)"
	ErrEntityNameFilterAlt        = " AND fg.primary_key = 'entity_name' AND fg.primary_value = ANY($2)"
	ErrFDReceiptDeletedFilter     = "AND COALESCE(f.is_deleted, FALSE) = FALSE"
	ErrInvalidFDModule            = "module %q is not a cross-stage FD module"
	ErrInvalidMFModule            = "module %q is not a cross-stage MF module"
	ErrEntityIDNotAuthorized      = "Entity ID '%s' is not within your authorized access scope."
	ErrScheduleConfigLookupFailed = "Schedule config lookup failed: "
	ErrReceiptDateFilter          = " AND receipt_date>=$%d"
	ErrReceiptDateFilterEnd       = " AND receipt_date<=$%d"
	ErrFDNotFound1                = "FD master record not found"
	ErrBankNotApproved1           = "Bank '%s' is not within your approved bank scope."
	ErrBankIDFilterAlt            = "r.bank_id"
	ErrEntityIDFilterAlt          = "r.entity_id"
	ErrTDSIDFilterAlt             = "t.bank_id"
	ErrTDSEntityIDFilterAlt       = "t.entity_id"
	ErrEntityNameFilterAlt2       = " AND (COALESCE(i.entity_name,'') = '' OR i.entity_name = ANY($%d::text[]))"
	ErrSchemeIDFilterAlt          = " AND i.scheme_id = ANY($%d::text[])"
	ErrFolioIDFilterAlt           = " AND (COALESCE(i.folio_id,'') = '' OR i.folio_id = ANY($%d::text[]))"
	ErrDematIDFilterAlt           = " AND (COALESCE(i.demat_id,'') = '' OR i.demat_id = ANY($%d::text[]))"
	ErrFDBookingsFilter           = "FD Bookings"
	ErrCashTransactionsFilter     = "Cash Transactions"
)

// Date formats
const (
	DateTimeFormat         = "2006-01-02 15:04:05"
	DateFormat             = "2006-01-02"
	DateFormatAlt          = "02-01-2006"
	DateFormatISO          = "2006-01-02T15:04:05"
	DateFormatSlash        = "02/Jan/2006"
	DateFormatDash         = "02-Jan-2006"
	DateFormatCustom       = "01-01-1"
	DateFormatYearMonth    = "2006-01"
	DateRange0To1Days      = "0-1 Days"
	DateRange2To3Days      = "2-3 Days"
	DateRangeMoreThan3Days = ">3 Days"
	DateMax                = "2099-12-31"
	DateMin                = "2000-01-01"
	DateMaxCustom          = "2025-01-31"
	DateMaxCustom2         = "2027-01-31"
	DateMaxCustom3         = "2025-02-28"
	DateMaxCustom4         = "2025-03-31"
	DateMaxCustom5         = "2025-04-30"
	DayCountConvActual365  = "Actual/365"
)

const (
	ErrPlanIDRequired                   = "plan_id is required"
	ErrFailedToGetGroups                = "failed to get groups: "
	ErrFailedToScanGroupID              = "failed to scan group ID: "
	ErrPrefixPayable                    = "TR-PAY-"
	ErrPrefixReceivable                 = "TR-REC-"
	FormatTransactionID                 = "%s-%d-%d"
	ErrFailedToInsertMonthlyProjections = "Failed to insert monthly projections: "
	ErrFailedToBeginTransaction         = "failed to begin transaction: "
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
	ErrUserIDAndInitiationIDsRequired     = "user_id and initiation_ids required"
	ErrFailedToUploadFileToS3             = "Failed to upload file to S3"
	ErrFailedToCommitTransaction          = "Database commit error"
	ErrUpdateConfirmationFailed           = "Update confirmation failed"
	ErrUserIDAndCodeRequired              = "user_id and code are required"
	ErrInvalidCode                        = "invalid code"
	ErrClosingBalance                     = "closing balance"
	ErrAllRowsFailedValidation            = "All rows failed validation"
	ErrBatchAuditFailed                   = "Batch audit failed"
	ErrInvalidFilter                      = "Invalid filter: "
	ErrBankStatementFileAlreadyUploaded   = "This bank statement file was already uploaded earlier. Please upload a different file."
	ErrLedgerIDRequired                   = "ledger_id (or both run_id + fd_id) is required"
	ErrNoBankIDsProvided                  = "No bank_ids provided"
	ErrNoProviderIDsProvided              = "No provider_ids provided"
	ErrNoCCPCSDIDsProvided                = "No ccp_csd_ids provided"
	ErrNoCounterpartyIDsProvided          = "No counterparty_ids provided"
	ErrCounterpartyMasterTable            = "apibox.counterparty_master"
	ErrNoErpSystemIDsProvided             = "No erp_system_ids provided"
	ErrAuditCounterpartyMasterTable       = "apibox.audit_counterparty_master"
	ErrNoExchangeIDsProvided              = "No exchange_ids provided"
	ErrCounterpartyServiceTable           = "apibox_svc.counterparty"
	ErrAuditCounterpartyServiceTable      = "apibox_svc.audit_counterparty"
	ErrCounterpartyIDsRequired            = "counterparty_ids is required"
	ErrCounterpartyNotFound1              = "counterparty not found"
	ErrNoPaymentNetworkIDsProvided        = "No payment_network_ids provided"
	ErrLoadConfirmationSchemaFailed       = "Load confirmation schema failed"
)
const (
	StatusCodeAwaitingApproval = "Awaiting-Approval"
	StatusCodeDeleteApproval   = "Delete-Approval"
)

// General approval workflow status constants (uppercase, used across all modules).
// Use these for processing_status, approval_status, and similar workflow state fields.
const (
	StatusApproved              = "APPROVED"
	StatusRejected              = "REJECTED"
	StatusPending               = "PENDING"
	StatusPendingApproval       = "PENDING_APPROVAL"
	StatusPendingEditApproval   = "PENDING_EDIT_APPROVAL"
	StatusPendingDeleteApproval = "PENDING_DELETE_APPROVAL"
	StatusActive                = "ACTIVE"
	StatusInactive              = "INACTIVE"
)

// User account status values (title-case, stored in the users.status column).
const (
	UserStatusApproved = "Approved"
	UserStatusPending  = "pending"
	UserStatusDisabled = "Disabled"
)

// Audit action type constants — used in RecordAction / RecordDecision calls.
const (
	AuditActionCreate  = "CREATE"
	AuditActionEdit    = "EDIT"
	AuditActionDelete  = "DELETE"
	AuditActionConfirm = "CONFIRM"
	AuditActionApprove = "APPROVE"
	AuditActionReject  = "REJECT"
)

// Forward booking processing_status values (set only after confirmation).
const (
	FwdProcessingStatusPendingApproval       = StatusPendingApproval // PENDING_APPROVAL
	FwdProcessingStatusApproved              = StatusApproved        // APPROVED
	FwdProcessingStatusRejected              = StatusRejected        // REJECTED
	FwdProcessingStatusPendingDeleteApproval = StatusPendingDeleteApproval
	FwdProcessingStatusPendingEditApproval   = StatusPendingEditApproval
	// Legacy aliases still present in older rows / SQL checks.
	FwdProcessingStatusPending        = FwdProcessingStatusPendingApproval
	FwdProcessingStatusDeleteApproval = "Delete-approval"
)

// Forward booking status values (lifecycle).
const (
	FwdStatusDraft                     = "DRAFT"
	FwdStatusPendingConfirmation       = "PENDING_CONFIRMATION"
	FwdStatusConfirmed                 = "CONFIRMED"
	FwdStatusPendingConfirmationLegacy = "Pending Confirmation"
	FwdStatusConfirmedLegacy           = "Confirmed"
)

// Forward booking audit action types
const (
	FwdActionTypeEdit    = AuditActionEdit
	FwdActionTypeDelete  = AuditActionDelete
	FwdActionTypeConfirm = AuditActionConfirm
)

const (
	HeaderAccessControlAllowOrigin       = "Access-Control-Allow-Origin"
	HeaderAccessControlAllowMethods      = "Access-Control-Allow-Methods"
	HeaderAccessControlAllowMethodsValue = "GET, POST, PUT, DELETE, OPTIONS"
	HeaderAccessControlAllowHeaders      = "Access-Control-Allow-Headers"
	HeaderAccessControlAllowHeadersValue = "Content-Type, Authorization"
)

const (
	QuerryCounterpartyName                = "COALESCE(m.counterparty_name, 'Generic')"
	QuerryGeneric                         = "'Generic'"
	VendorPayment                         = "'Vendor Payment'"
	ExposureBucketing                     = "exposure-bucketing"
	ExposureUpload                        = "exposure-upload"
	QuerryPendingFields                   = " | pending_fields:"
	QuerryNoPendingAuditFound             = ": no pending audit found"
	QuerryMakerCheckerViolation           = ": maker-checker violation"
	QuerryEntity                          = " AND eh.entity = ANY($%d)"
	QuerryHighUnhedgedExposure            = "High Unhedged Exposure"
	QuerryWhereClause                     = "WHERE eh.entity = ANY($2) AND"
	QuerryCurrency2                       = " AND eh.currency = ANY($%d)"
	QuerryEntityName                      = " AND cpi.entity_name = $%d"
	QuerryCurrencyCode                    = " AND cp.currency_code = $%d"
	QuerryFplCurrency                     = " AND fpl.currency = $%d"
	QuerryFilterGroup                     = " AND fg.primary_key = 'entity_name' AND fg.primary_value = $%d"
	QuerryBankName                        = "(b.bank_name IS NULL OR b.bank_name = ANY($%d))"
	QuerryCurrency                        = "(a.currency IS NULL OR a.currency = ANY($%d))"
	QuerryEntityNameLower                 = " AND lower(trim(c.entity_name)) = ANY($%d)"
	QuerryBankNameLower                   = " AND lower(trim(c.bank_name)) = ANY($%d)"
	QuerryAuditBookingRequest             = "investment.fd_audit_booking_request"
	QuerryAuditInterestReceipt            = "investment.fd_interest_receipt_audit"
	QuerryBookingRequest                  = "investment.fd_booking_request"
	QuerryRateNegotiation                 = "investment.fd_rate_negotiation"
	QuerryAuditRateNegotiation            = "investment.fd_audit_rate_negotiation"
	QuerryConfirmation                    = "investment.fd_confirmation"
	QuerryAuditConfirmation               = "investment.fd_audit_confirmation"
	QuerryMaster                          = "investment.fd_master"
	QuerryCashflowSchedule                = "investment.fd_cashflow_schedule"
	QuerryCashflow                        = "investment.fd_cashflow"
	QuerryInterestReceipt                 = "investment.fd_interest_receipt"
	QuerryMasterCashflowSchedule          = "investment.fd_master_cashflow_schedule"
	QuerryAuditClosureRequest             = "investment.fd_audit_closure_request"
	QuerryAuditMaster                     = "investment.fd_audit_master"
	QuerryAccrualScheduleConfig           = "investment.fd_accrual_schedule_config"
	QuerryAccrualScheduleConfigAudit      = "investment.fd_accrual_schedule_config_audit"
	QuerryClosureRequest                  = "investment.fd_closure_request"
	QuerryReceiptExceptionAudit           = "investment.fd_receipt_exception_audit"
	QuerryReceiptException                = "investment.fd_receipt_exception"
	QuerryAuditCashflowSchedule           = "investment.fd_audit_cashflow_schedule"
	QuerryTDSReceipt                      = "investment.fd_tds_receipt"
	QuerryAuditTDSReceipt                 = "investment.fd_tds_receipt_audit"
	QuerryAuditReceiptException           = "investment.fd_receipt_exception_audit"
	QuerryAuditClosureInitiate            = "cimplr.fd_closure_initiate_audit"
	QuerryAuditClosureConfirm             = "cimplr.fd_closure_confirm"
	QuerryAuditClosureConfirmAudit        = "cimplr.fd_closure_confirm_audit"
	ErrFDNotFound                         = "FD not found"
	ErrClosureCalculationFailed           = "closure calculation failed: "
	ErrCalculationSnapshotFailed          = "calculation snapshot failed: "
	QuerryClosureInitiate                 = "cimplr.fd_closure_initiate"
	QuerryAvailableBalance                = "available balance"
	FormatInvestmentID                    = "INV-%s"
	FormatFDActivation                    = "FD activation %s"
	ErrLoadFDRecord                       = "load FD record: %w"
	TranID                                = "Tran. Id"
	FormatInvestmentName                  = "Investment - %s"
	FormatFDInvestment                    = "FD Investment - "
	FormatMasterCashflowCategory          = "master-cashflow-category"
	FormatMasterCostProfitCenter          = "master-costprofit-center"
	FormatInterestIncome                  = "Interest Income - FD"
	FormatMTMGain                         = "MTM gain - %s"
	FormatMTMLoss                         = "MTM loss - %s"
	FormatLogMore                         = "  ... (%d more) ...\n"
	StorageObjectURLFormat                = "%s/storage/v1/object/%s/%s"
	QuerrySweepID                         = " AND l.sweep_id = $%d"
	QuerryInitiationID                    = " AND i.sweep_id = $%d"
	QuerryProcessingStatus                = " AND a.processing_status = $%d"
	QuerryEntityID                        = " AND entity_id=$%d"
	QuerryOrderByInitiationTime           = " ORDER BY GREATEST(COALESCE((SELECT requested_at FROM cimplrcorpsaas.auditactionsweepinitiation WHERE initiation_id = i.initiation_id ORDER BY requested_at DESC LIMIT 1), '1970-01-01'::timestamp), COALESCE((SELECT checker_at FROM cimplrcorpsaas.auditactionsweepinitiation WHERE initiation_id = i.initiation_id ORDER BY requested_at DESC LIMIT 1), '1970-01-01'::timestamp)) DESC"
	BearerPrefix                          = "Bearer "
	NBSP                                  = "\u00A0"
	TransactionPostedDate                 = "txn posted date"
	TransactionPostingDate                = "posting date"
	TransactionPostedDateAlt              = "posted date"
	TermLoans                             = "TERM LOANS"
	FundBased                             = "FUND BASED"
	NonFundBased                          = "NON FUND BASED"
	IntraCore                             = "INTRA-CORE"
	InterCore                             = "INTER-CORE"
	Unknown                               = "(unknown)"
	QuerryInsertBankConfig                = "INSERT INTO investment.fd_bank_config_master (%s) VALUES %s RETURNING config_id"
	ErrNoConfigIDsProvided                = "No config_ids provided"
	ErrNoRateCardIDsProvided              = "No rate_card_ids provided"
	ErrNoDayCountCodesProvided            = "No day_count_codes provided"
	ErrNoPenaltyIDsProvided               = "No penalty_ids provided"
	FormatIsEnabled                       = "is_enabled = $%d"
	FormatRetryMax                        = "retry_max = $%d"
	FormatRetryBackoffSecs                = "retry_backoff_secs = $%d"
	FormatPriorityLevel                   = "priority_level = $%d"
	FormatUpdatedBy                       = "updated_by = $%d"
	FormatUpdatedAt                       = "updated_at = now()"
	ErrEyeAuditFailed                     = "Eye audit failed"
	ErrMemberAuditFailed                  = "Member audit failed"
	ErrApprovalMatrixNotFound             = "Approval matrix not found"
	ErrMatrixIDsCannotBeEmpty             = "matrix_ids cannot be empty"
	ErrLookupFailed                       = "Lookup failed"
	CheckConstraint                       = "check constraint"
	None                                  = "NONE"
	TransactionDate                       = "transaction date"
	TransactionDateAlt                    = "Transaction Date"
	ValueDate                             = "value date"
	ValueDateAlt                          = "Value Date"
	TransactionRemarks                    = "transaction remarks"
	WithdrawalAmountINR                   = "Withdrawal Amt (INR)"
	DepositAmountINR                      = "Deposit Amt (INR)"
	BalanceINR                            = "Balance (INR)"
	CreditAmount                          = "credit amt"
	DebitAmount                           = "debit amt"
	ClosingBalance                        = "closing balance"
	BalanceCarriedForward                 = "balance carried forward"
	BalanceBroughtForward                 = "balance brought forward"
	ErrFailedToInsertBankStatement        = "failed to insert bank statement: %w"
	ErrNoAccessToBankStatement            = "No access to this bank statement"
	ErrFailedToInsertAuditAction          = "failed to insert audit action: %w"
	ErrFailedToReadProjectionAuditHistory = "failed to read projection download audit history"
	ErrMissingLatestAuditForInitiation    = "missing latest audit for initiation: "
	ProposalID                            = "p.proposal_id"
	MTMIDsRequired                        = "mtm_ids is required"
	ClosureIDsRequired                    = "closure_initiate_id or closure_confirm_id is required"
	ConfirmRecordNotFound                 = "confirm record not found"
	FDInvestmentPrefix                    = "FD-INVEST-"
	TDSReceivable                         = "TDS-RECEIVABLE"
	TDSReceivableLabel                    = "TDS Receivable"
	FDBooking                             = "fd-booking"
	FDBookingLabel                        = "FD Booking"
	FDConfirmation                        = "FD Confirmation"
	FDConfirmationLabel                   = "fd-confirmation"
	FDActivationLabel                     = "FD Activation"
	FDActivation                          = "fd-activation"
	FdmaturityLabel                       = "fd-maturity"
	FDMaturity                            = "FD Maturity"
	AccrualRun                            = "Accrual Run"
	FREQQ                                 = "FREQ-Q"
	FREQM                                 = "FREQ-M"
	PreviewPrefix                         = "PREVIEW-"
	FDInterestIncome                      = "FD-INT-INC-"
	ExceptionIDsRequired                  = "exception_ids is required"
	ReceiptNotFound                       = "Receipt not found"
	ConfirmationIDsRequired               = "confirmation_ids required"
	BatchIDsRequired                      = "batch_ids required"
	ErrFailedToDeleteStagedStatements     = "failed to delete staged statements"
	ErrConfirmedPrincipalAndInterest      = "confirmed_principal_amount and confirmed_interest_rate must be positive"
	FDAccrualEngine                       = "fd-accrual-engine"
	ExposureHeaders                       = "public.exposure_headers"
	UnionAll                              = "\nUNION ALL\n"
	ErrInvalidCrossStageModule            = "module %q is not a cross-stage FX exposure module"
	ErrInvalidCrossStageForwardModule     = "module %q is not a cross-stage FX forward module"
	ErrFailedToUploadFileMetadata         = "file upload metadata failed: %w"
	ErrFailedToDeleteItem                 = "delete %s failed: %s"
	AMCName                               = "s.amc_name"
	SchemeName                            = "s.scheme_name"
)

var (
	Nifty50   = "NIFTY 50"
	Nifty100  = "NIFTY 100"
	NiftyBank = "NIFTY BANK"
	NiftyIT   = "NIFTY IT"
)

// Investment source/suite labels
const (
	InvestmentSuite = "Investment Suite"
	RedemptionSuite = "Redemption Suite"
)

// Investment workflow error and log format strings
const (
	ErrHoldingsFetchFailed                = "Holdings fetch failed: "
	ErrPortfolioRefreshInvestmentApproval = "Failed to run global portfolio refresh after investment confirmation approval: %v"
	ErrPortfolioRefreshRedemptionApproval = "Failed to run global portfolio refresh after redemption confirmation approval: %v"
	ErrParseFmt                           = "parse: %w"
	ColTAAmfiSchemeCode                   = "ta.amfi_scheme_code"
)

const (
	NoDataTable = "<table><tr><td>No data</td></tr></table>"
	BgcolorF9   = "#f9f9f9"
)

// Email service audit / workflow constants
const (
	EmailSvcInboxAudit = "email_svc.inbox_audit"
)

// Dashboard builder error messages
const (
	ErrIDRequired              = "id is required"
	ErrDashboardNotFound       = "dashboard not found"
	ErrFailedToAssignDashboard = "failed to assign dashboard"
)

// Email mailbox test/route names and shared messages
const (
	RouteGoogleWorkspaceTest     = "google-workspace/test"
	RouteGraphTest               = "graph/test"
	RouteIMAPTest                = "imap/test"
	RouteOAuthStart              = "oauth/start"
	RouteOAuthTest               = "oauth/test"
	ErrMailProcessingUnavailable = "mail processing unavailable"
	ErrInvalidBody               = "invalid body"
	RedactedPlaceholder          = "********"
)

// Email inbox workflow error prefixes/suffixes
const (
	ErrImapPrefix             = ": imap: "
	ErrGraphPrefix            = ": graph: "
	ErrGoogleWorkspacePrefix  = ": google workspace: "
	ErrApprovalInstancePrefix = ": approval instance: "
	ErrNotFoundSuffix         = ": not found"
	ErrManualPollAdminOnly    = "manual poll is admin/QA only — mailboxes poll automatically after approval"
)

// FX v91 batch upload error messages
const (
	ErrBatchIDRequired = "batch_id is required"
	ErrInvalidBatchID  = "invalid batch_id"
	ErrBatchNotFound   = "batch not found"
)

// Master bulk-upload module keys and error prefixes
const (
	ModuleMasterDemat   = "master-demat"
	ModuleMasterDP      = "master-dp"
	ModuleMasterFolio   = "master-folio"
	ErrInsertPrefix     = "insert: "
	ErrAuditColonPrefix = "audit: "
)

// Master data validation messages
const (
	ErrHolidayAlreadyExists = "A holiday with this name already exists for the selected calendar."
)
