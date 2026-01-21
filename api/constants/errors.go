package constants

import "fmt"

// ============================================================================
// AUTHENTICATION & SESSION ERRORS
// ============================================================================

const (
	ErrMissingUserID  = "user_id is required in the request"
	ErrInvalidSession = "Your session has expired or is invalid. Please login again"
	ErrUnauthorized   = "You are not authorized to perform this action"
	ErrSessionExpired = "Your session has expired. Please login again"
)

// ============================================================================
// VALIDATION ERRORS - User & Business Unit
// ============================================================================

const (
	ErrUserNotFound             = "User not found in the system"
	ErrNoBusinessUnit           = "User has no business unit assigned. Please contact administrator"
	ErrNoAccessibleBusinessUnit = "No accessible business units found for your account"
	ErrInvalidBusinessUnit      = "Invalid business unit specified"
)

// ============================================================================
// VALIDATION ERRORS - Entities
// ============================================================================

const (
	ErrNoEntities         = "No accessible entities found for your business unit"
	ErrEntityNotFound     = "Entity not found or you don't have access to it"
	ErrEntityCreateFailed = "Failed to create entity. Please check if the entity already exists"
	ErrEntityUpdateFailed = "Failed to update entity. Please verify the entity ID and try again"
	ErrInvalidEntity      = "Invalid entity specified"
	ErrEntityNotApproved  = "Entity is pending approval"
	ErrEntityDeleted      = "Entity has been deleted"
	ErrRootEntityNotFound = "Root entity not found for your business unit. Please contact administrator"
)

// ============================================================================
// VALIDATION ERRORS - Currency
// ============================================================================

const (
	ErrNoCurrencies          = "No approved currencies found in the system"
	ErrInvalidCurrency       = "Invalid currency code specified"
	ErrCurrencyNotApproved   = "Currency is not approved for use"
	ErrCurrencyNotActive     = "Currency is not active in the system"
	ErrCurrencyDeleted       = "Currency has been deleted"
	ErrCurrencyRequired      = "Currency code is required"
	ErrCurrencyNotFound      = "Currency not found in the system"
	ErrCurrencyAlreadyExists = "Currency with code %s already exists in the system"
	ErrCurrencyCreateFailed  = "Failed to create currency. Please check if the currency code already exists"
	ErrCurrencyUpdateFailed  = "Failed to update currency. Please verify the currency ID and try again"
	ErrCurrencyNotAllowed    = "Currency not allowed for this operation"
)

// ============================================================================
// VALIDATION ERRORS - Bank
// ============================================================================

const (
	ErrNoBanks               = "No approved banks found in the system"
	ErrInvalidBank           = "Invalid bank specified"
	ErrBankNotFound          = "Bank not found in the system"
	ErrBankNotApproved       = "Bank is not approved for use"
	ErrBankNotActive         = "Bank is not active in the system"
	ErrBankDeleted           = "Bank has been deleted"
	ErrBankRequired          = "Bank name or ID is required"
	ErrBankAlreadyExists     = "Bank with name %s already exists in the system"
	ErrBankCreateFailed      = "Failed to create bank. Please check if the bank already exists"
	ErrBankUpdateFailed      = "Failed to update bank. Please verify the bank ID and try again"
	ErrBankInvalidOrInactive = "Invalid or inactive bank"
	ErrBankNotAllowed        = "Bank not allowed"
)

// ============================================================================
// VALIDATION ERRORS - Bank Account
// ============================================================================

const (
	ErrNoBankAccounts            = "No bank accounts found"
	ErrNoBankAccountsForEntity   = "No bank accounts found for the selected entity"
	ErrNoBankAccountsForBank     = "No bank accounts found for the selected bank"
	ErrBankAccountNotFound       = "Bank account not found or you don't have access to it"
	ErrBankAccountNotApproved    = "Bank account is pending approval"
	ErrBankAccountNotActive      = "Bank account is not active"
	ErrBankAccountDeleted        = "Bank account has been deleted"
	ErrBankAccountRequired       = "Bank account information is required"
	ErrBankAccountAlreadyExists  = "Bank account already exists in the system"
	ErrBankAccountCreateFailed   = "Failed to create bank account"
	ErrBankAccountUpdateFailed   = "'. Please contact your administrator."
	ErrDBMasterBankAccountLookup = "db error while looking up account in masterbankaccount: %w"
)

const (
	ErrBankStatementAccessDenied = "No access to this bank statement"
)

// ============================================================================
// VALIDATION ERRORS - Cash Flow Category
// ============================================================================

const (
	ErrNoCashFlowCategories    = "No approved cash flow categories found"
	ErrInvalidCashFlowCategory = "Invalid cash flow category specified"
	ErrCategoryNotApproved     = "Category is not approved for use"
	ErrCategoryNotActive       = "Category is not active"
	ErrCategoryDeleted         = "Category has been deleted"
)

// ============================================================================
// VALIDATION ERRORS - Payable/Receivable
// ============================================================================

const (
	ErrNoPayableReceivable      = "No payable/receivable types found"
	ErrInvalidPayableReceivable = "Invalid payable/receivable type specified"
	ErrPayRecNotApproved        = "Payable/receivable type is not approved"
)

// ============================================================================
// VALIDATION ERRORS - Counterparty
// ============================================================================

const (
	ErrNoCounterparties        = "No counterparties found"
	ErrCounterpartyNotFound    = "Counterparty not found or you don't have access to it"
	ErrCounterpartyNotApproved = "Counterparty is not approved for use"
	ErrCounterpartyDeleted     = "Counterparty has been deleted"
)

// ============================================================================
// VALIDATION ERRORS - GL Account
// ============================================================================

const (
	ErrNoGLAccounts         = "No GL accounts found"
	ErrGLAccountNotFound    = "GL account not found or you don't have access to it"
	ErrGLAccountNotApproved = "GL account is not approved for use"
	ErrGLAccountDeleted     = "GL account has been deleted"
)

// ============================================================================
// VALIDATION ERRORS - Investment Masters (AMC, Scheme, etc.)
// ============================================================================

const (
	// AMC Errors
	ErrNoAMCs         = "No asset management companies (AMCs) found"
	ErrAMCNotFound    = "AMC not found in the system"
	ErrAMCNotApproved = "AMC is not approved for use"
	ErrAMCNotActive   = "AMC is not active"
	ErrAMCDeleted     = "AMC has been deleted"

	// Scheme Errors
	ErrNoSchemes         = "No schemes found"
	ErrSchemeNotFound    = "Scheme not found in the system"
	ErrSchemeNotApproved = "Scheme is not approved for use"
	ErrSchemeNotActive   = "Scheme is not active"
	ErrSchemeDeleted     = "Scheme has been deleted"

	// DP (Depository Participant) Errors
	ErrNoDPs         = "No depository participants found"
	ErrDPNotFound    = "Depository participant not found"
	ErrDPNotApproved = "Depository participant is not approved for use"
	ErrDPNotActive   = "Depository participant is not active"

	// Demat Errors
	ErrNoDemats         = "No demat accounts found"
	ErrDematNotFound    = "Demat account not found or you don't have access to it"
	ErrDematNotApproved = "Demat account is not approved for use"
	ErrDematNotActive   = "Demat account is not active"

	// Folio Errors
	ErrNoFolios         = "No folios found"
	ErrFolioNotFound    = "Folio not found or you don't have access to it"
	ErrFolioNotApproved = "Folio is not approved for use"
	ErrFolioNotActive   = "Folio is not active"
)

// ============================================================================
// DATABASE OPERATION ERRORS
// ============================================================================

const (
	ErrDatabaseConnection      = "Database connection failed. Please try again later"
	ErrQueryFailed             = "Database query failed. Please contact support if this persists"
	ErrTransactionFailed       = "Transaction failed. Please try again"
	ErrDuplicateEntry          = "This entry already exists in the system"
	ErrConstraintViolation     = "Operation violates data constraints"
	ErrRecordNotFound          = "Record not found in the database"
	ErrDatabaseScanFailed      = "Failed to read database results"
	ErrDatabaseQueryFailed     = "Database query failed. Please try again or contact support"
	ErrDatabaseDeleteFailed    = "Failed to delete record from database"
	ErrAuditLogFailed          = "Failed to create audit log entry"
	ErrTransactionCommitFailed = "Failed to save changes. Please try again"
)

// ============================================================================
// FILE UPLOAD ERRORS
// ============================================================================

const (
	ErrFileUploadFailed  = "File upload failed. Please check the file format and try again"
	ErrInvalidFileFormat = "Invalid file format. Please upload a valid file"
	ErrFileTooLarge      = "File size exceeds the maximum limit"
	ErrFileParsingFailed = "Failed to parse file contents. Please check the file format"
	ErrEmptyFile         = "Uploaded file is empty"
	ErrInvalidHeaders    = "File has invalid or missing column headers"
	ErrInvalidDataRow    = "Invalid data found in row %d: %s"
)

// ============================================================================
// AUDIT & APPROVAL ERRORS
// ============================================================================

const (
	ErrPendingApproval      = "This record is pending approval"
	ErrAlreadyApproved      = "This record has already been approved"
	ErrAlreadyRejected      = "This record has already been rejected"
	ErrNoApprovalPermission = "You don't have permission to approve/reject this record"
	ErrCannotModifyApproved = "Cannot modify an approved record"
	ErrCannotDeleteApproved = "Cannot delete an approved record"
)

// ============================================================================
// INPUT VALIDATION ERRORS
// ============================================================================

const (
	ErrMissingRequiredField = "Required field '%s' is missing"
	ErrInvalidFieldValue    = "Invalid value for field '%s': %s"
	ErrInvalidDateFormat    = "Invalid date format for '%s'. Expected format: YYYY-MM-DD"
	ErrInvalidAmount        = "Invalid amount specified"
	ErrNegativeAmount       = "Amount cannot be negative"
	ErrInvalidID            = "Invalid ID specified"
)

// ============================================================================
// GENERAL ERRORS
// ============================================================================

const (
	ErrInternalServer  = "Internal server error. Please contact support"
	ErrOperationFailed = "Operation failed. Please try again"
	ErrNoDataFound     = "No data found matching your criteria"
	ErrInvalidRequest  = "Invalid request. Please check your input"
	ErrNotImplemented  = "This feature is not yet implemented"
)

// ============================================================================
// SUCCESS MESSAGES
// ============================================================================

const (
	SuccessCreated  = "Record created successfully"
	SuccessUpdated  = "Record updated successfully"
	SuccessDeleted  = "Record deleted successfully"
	SuccessApproved = "Record approved successfully"
	SuccessRejected = "Record rejected successfully"
	SuccessUploaded = "File uploaded successfully. %d records processed"
)

// ============================================================================
// HELPER FUNCTIONS TO FORMAT ERRORS WITH CONTEXT
// ============================================================================

// FormatError formats an error message with additional context
func FormatError(baseError string, context ...interface{}) string {
	if len(context) == 0 {
		return baseError
	}
	return fmt.Sprintf(baseError, context...)
}

// FormatRowError formats an error for a specific data row
func FormatRowError(rowNum int, reason string) string {
	return fmt.Sprintf(ErrInvalidDataRow, rowNum, reason)
}

// FormatFieldError formats an error for a specific field
func FormatFieldError(fieldName string, reason string) string {
	return fmt.Sprintf(ErrInvalidFieldValue, fieldName, reason)
}

// FormatMissingFieldError formats a missing field error
func FormatMissingFieldError(fieldName string) string {
	return fmt.Sprintf(ErrMissingRequiredField, fieldName)
}
