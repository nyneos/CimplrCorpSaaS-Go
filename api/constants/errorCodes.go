package constants

// Machine-readable error codes for the error.code field in API responses.
// Each constant maps to a human-readable message in errors.go.
const (
	// Auth & Session
	CodeAuthMissingUserID  = "AUTH_MISSING_USER_ID"
	CodeAuthInvalidSession = "AUTH_INVALID_SESSION"
	CodeAuthUnauthorized   = "AUTH_UNAUTHORIZED"
	CodeAuthSessionExpired = "AUTH_SESSION_EXPIRED"
	CodeAuthForbidden      = "AUTH_FORBIDDEN"

	// User & Business Unit
	CodeUserNotFound             = "USER_NOT_FOUND"
	CodeNoBusinessUnit           = "NO_BUSINESS_UNIT"
	CodeNoAccessibleBusinessUnit = "NO_ACCESSIBLE_BUSINESS_UNIT"
	CodeInvalidBusinessUnit      = "INVALID_BUSINESS_UNIT"

	// Entity
	CodeNoEntities         = "NO_ENTITIES"
	CodeEntityNotFound     = "ENTITY_NOT_FOUND"
	CodeEntityCreateFailed = "ENTITY_CREATE_FAILED"
	CodeEntityUpdateFailed = "ENTITY_UPDATE_FAILED"
	CodeInvalidEntity      = "INVALID_ENTITY"
	CodeEntityNotApproved  = "ENTITY_NOT_APPROVED"
	CodeEntityDeleted      = "ENTITY_DELETED"
	CodeRootEntityNotFound = "ROOT_ENTITY_NOT_FOUND"

	// Currency
	CodeNoCurrencies          = "NO_CURRENCIES"
	CodeInvalidCurrency       = "INVALID_CURRENCY"
	CodeCurrencyNotApproved   = "CURRENCY_NOT_APPROVED"
	CodeCurrencyNotActive     = "CURRENCY_NOT_ACTIVE"
	CodeCurrencyDeleted       = "CURRENCY_DELETED"
	CodeCurrencyRequired      = "CURRENCY_REQUIRED"
	CodeCurrencyNotFound      = "CURRENCY_NOT_FOUND"
	CodeCurrencyAlreadyExists = "CURRENCY_ALREADY_EXISTS"
	CodeCurrencyCreateFailed  = "CURRENCY_CREATE_FAILED"
	CodeCurrencyUpdateFailed  = "CURRENCY_UPDATE_FAILED"
	CodeCurrencyNotAllowed    = "CURRENCY_NOT_ALLOWED"

	// Bank
	CodeNoBanks               = "NO_BANKS"
	CodeInvalidBank           = "INVALID_BANK"
	CodeBankNotFound          = "BANK_NOT_FOUND"
	CodeBankNotApproved       = "BANK_NOT_APPROVED"
	CodeBankNotActive         = "BANK_NOT_ACTIVE"
	CodeBankDeleted           = "BANK_DELETED"
	CodeBankRequired          = "BANK_REQUIRED"
	CodeBankAlreadyExists     = "BANK_ALREADY_EXISTS"
	CodeBankCreateFailed      = "BANK_CREATE_FAILED"
	CodeBankUpdateFailed      = "BANK_UPDATE_FAILED"
	CodeBankInvalidOrInactive = "BANK_INVALID_OR_INACTIVE"
	CodeBankNotAllowed        = "BANK_NOT_ALLOWED"

	// Bank Account
	CodeNoBankAccounts            = "NO_BANK_ACCOUNTS"
	CodeBankAccountNotFound       = "BANK_ACCOUNT_NOT_FOUND"
	CodeBankAccountNotApproved    = "BANK_ACCOUNT_NOT_APPROVED"
	CodeBankAccountNotActive      = "BANK_ACCOUNT_NOT_ACTIVE"
	CodeBankAccountDeleted        = "BANK_ACCOUNT_DELETED"
	CodeBankAccountRequired       = "BANK_ACCOUNT_REQUIRED"
	CodeBankAccountAlreadyExists  = "BANK_ACCOUNT_ALREADY_EXISTS"
	CodeBankAccountCreateFailed   = "BANK_ACCOUNT_CREATE_FAILED"
	CodeBankAccountUpdateFailed   = "BANK_ACCOUNT_UPDATE_FAILED"
	CodeBankStatementAccessDenied = "BANK_STATEMENT_ACCESS_DENIED"

	// Cash Flow Category
	CodeNoCashFlowCategories    = "NO_CASH_FLOW_CATEGORIES"
	CodeInvalidCashFlowCategory = "INVALID_CASH_FLOW_CATEGORY"
	CodeCategoryNotApproved     = "CATEGORY_NOT_APPROVED"
	CodeCategoryNotActive       = "CATEGORY_NOT_ACTIVE"
	CodeCategoryDeleted         = "CATEGORY_DELETED"

	// Payable/Receivable
	CodeNoPayableReceivable      = "NO_PAYABLE_RECEIVABLE"
	CodeInvalidPayableReceivable = "INVALID_PAYABLE_RECEIVABLE"
	CodePayRecNotApproved        = "PAY_REC_NOT_APPROVED"

	// Counterparty
	CodeNoCounterparties        = "NO_COUNTERPARTIES"
	CodeCounterpartyNotFound    = "COUNTERPARTY_NOT_FOUND"
	CodeCounterpartyNotApproved = "COUNTERPARTY_NOT_APPROVED"
	CodeCounterpartyDeleted     = "COUNTERPARTY_DELETED"

	// GL Account
	CodeNoGLAccounts         = "NO_GL_ACCOUNTS"
	CodeGLAccountNotFound    = "GL_ACCOUNT_NOT_FOUND"
	CodeGLAccountNotApproved = "GL_ACCOUNT_NOT_APPROVED"
	CodeGLAccountDeleted     = "GL_ACCOUNT_DELETED"

	// Investment — AMC
	CodeNoAMCs         = "NO_AMCS"
	CodeAMCNotFound    = "AMC_NOT_FOUND"
	CodeAMCNotApproved = "AMC_NOT_APPROVED"
	CodeAMCNotActive   = "AMC_NOT_ACTIVE"
	CodeAMCDeleted     = "AMC_DELETED"

	// Investment — Scheme
	CodeNoSchemes         = "NO_SCHEMES"
	CodeSchemeNotFound    = "SCHEME_NOT_FOUND"
	CodeSchemeNotApproved = "SCHEME_NOT_APPROVED"
	CodeSchemeNotActive   = "SCHEME_NOT_ACTIVE"
	CodeSchemeDeleted     = "SCHEME_DELETED"

	// Investment — DP
	CodeNoDPs         = "NO_DPS"
	CodeDPNotFound    = "DP_NOT_FOUND"
	CodeDPNotApproved = "DP_NOT_APPROVED"
	CodeDPNotActive   = "DP_NOT_ACTIVE"

	// Investment — Demat
	CodeNoDemats         = "NO_DEMATS"
	CodeDematNotFound    = "DEMAT_NOT_FOUND"
	CodeDematNotApproved = "DEMAT_NOT_APPROVED"
	CodeDematNotActive   = "DEMAT_NOT_ACTIVE"

	// Investment — Folio
	CodeNoFolios         = "NO_FOLIOS"
	CodeFolioNotFound    = "FOLIO_NOT_FOUND"
	CodeFolioNotApproved = "FOLIO_NOT_APPROVED"
	CodeFolioNotActive   = "FOLIO_NOT_ACTIVE"

	// Database
	CodeDatabaseConnection      = "DB_CONNECTION_FAILED"
	CodeQueryFailed             = "DB_QUERY_FAILED"
	CodeTransactionFailed       = "DB_TRANSACTION_FAILED"
	CodeDuplicateEntry          = "DB_DUPLICATE_ENTRY"
	CodeConstraintViolation     = "DB_CONSTRAINT_VIOLATION"
	CodeRecordNotFound          = "DB_RECORD_NOT_FOUND"
	CodeDatabaseScanFailed      = "DB_SCAN_FAILED"
	CodeDatabaseDeleteFailed    = "DB_DELETE_FAILED"
	CodeAuditLogFailed          = "DB_AUDIT_LOG_FAILED"
	CodeTransactionCommitFailed = "DB_COMMIT_FAILED"

	// File Upload
	CodeFileUploadFailed  = "FILE_UPLOAD_FAILED"
	CodeInvalidFileFormat = "FILE_INVALID_FORMAT"
	CodeFileTooLarge      = "FILE_TOO_LARGE"
	CodeFileParsingFailed = "FILE_PARSING_FAILED"
	CodeEmptyFile         = "FILE_EMPTY"
	CodeInvalidHeaders    = "FILE_INVALID_HEADERS"
	CodeInvalidDataRow    = "FILE_INVALID_DATA_ROW"

	// Audit & Approval
	CodePendingApproval      = "APPROVAL_PENDING"
	CodeAlreadyApproved      = "APPROVAL_ALREADY_APPROVED"
	CodeAlreadyRejected      = "APPROVAL_ALREADY_REJECTED"
	CodeNoApprovalPermission = "APPROVAL_NO_PERMISSION"
	CodeCannotModifyApproved = "APPROVAL_CANNOT_MODIFY"
	CodeCannotDeleteApproved = "APPROVAL_CANNOT_DELETE"

	// Input Validation
	CodeMissingRequiredField = "INPUT_MISSING_FIELD"
	CodeInvalidFieldValue    = "INPUT_INVALID_VALUE"
	CodeInvalidDateFormat    = "INPUT_INVALID_DATE"
	CodeInvalidAmount        = "INPUT_INVALID_AMOUNT"
	CodeNegativeAmount       = "INPUT_NEGATIVE_AMOUNT"
	CodeInvalidID            = "INPUT_INVALID_ID"

	// General
	CodeInternalServer  = "INTERNAL_SERVER_ERROR"
	CodeOperationFailed = "OPERATION_FAILED"
	CodeNoDataFound     = "NO_DATA_FOUND"
	CodeInvalidRequest  = "INVALID_REQUEST"
	CodeNotImplemented  = "NOT_IMPLEMENTED"
)
