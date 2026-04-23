package projection

import (
	// "errors"
	"CimplrCorpSaas/api/constants"
	"strings"
)

// parseConstraintError converts PostgreSQL constraint error messages to human-readable format
func parseConstraintError(err error) string {
	if err == nil {
		return ""
	}

	errStr := err.Error()

	// Handle unique constraint violations
	if strings.Contains(errStr, "unique constraint") || strings.Contains(errStr, "duplicate key") {

		// Cashflow proposal name uniqueness
		if strings.Contains(errStr, "uniq_cashflow_proposal_name") {
			return "Proposal name already exists. Each proposal must have a unique name. Please choose a different name for your cashflow proposal."
		}

		// Cashflow proposal item uniqueness - very detailed since this is a complex constraint
		if strings.Contains(errStr, "uniq_cashflow_proposal_item_full") {
			return "A cashflow item with identical details already exists in this proposal. Please modify one or more of the following to make it unique: description, cashflow type (Inflow/Outflow), category, currency, expected amount, recurring settings, maturity date, bank details, or entity name."
		}

		// Index-based constraints (these shouldn't normally be user-visible but handle just in case)
		if strings.Contains(errStr, "idx_audit_cashflow_proposal_latest_per_proposal") {
			return "Audit record conflict detected. Please contact system administrator."
		}

		if strings.Contains(errStr, "idx_audit_cashflow_proposal_status_time") {
			return "Processing status conflict detected. Please refresh and try again."
		}

		// General duplicate key error
		if strings.Contains(errStr, "duplicate key") {
			return "This record already exists. Please check for duplicates and modify the conflicting details."
		}
	}

	// Handle foreign key violations
	if strings.Contains(errStr, "foreign key constraint") {
		if strings.Contains(errStr, "category_id") {
			return "Invalid category selected. The category may have been deleted or you don't have permission to use it. Please choose a valid category from the available options."
		}
		if strings.Contains(errStr, "proposal_id") {
			return "Invalid proposal reference. The proposal may have been deleted or moved. Please refresh the page and try again."
		}
		if strings.Contains(errStr, "entity_name") {
			return "Invalid entity specified. The entity may not exist or you may not have permission to create cashflows for it. Please verify the entity name."
		}
		if strings.Contains(errStr, "department_id") {
			return "Invalid department selected. Please choose a valid department from your organization."
		}
		return "Referenced record not found. Please verify that all selected options (category, entity, department) are valid and accessible."
	}

	// Handle check constraint violations
	if strings.Contains(errStr, constants.CheckConstraint) {
		if strings.Contains(errStr, "cashflow_type") {
			return "Cashflow type must be either 'Inflow' or 'Outflow'. Please select a valid cashflow direction."
		}
		if strings.Contains(errStr, "expected_amount") || strings.Contains(errStr, "amount") {
			return "Expected amount must be a positive number greater than zero. Please enter a valid monetary amount."
		}
		if strings.Contains(errStr, "effective_date") {
			return "Effective date must be a valid date. Please ensure the date is in the correct format (YYYY-MM-DD) and not in the distant past."
		}
		if strings.Contains(errStr, "currency_code") {
			return "Invalid currency code. Please use a valid 3-letter currency code (e.g., USD, EUR, INR)."
		}
		if strings.Contains(errStr, "recurrence_frequency") {
			return "Invalid recurrence frequency. Please choose from: Monthly, Quarterly, or Yearly."
		}
		return "Data validation failed. Please check that all numeric fields contain valid positive numbers and all dates are properly formatted."
	}

	// Handle not null violations
	if strings.Contains(errStr, "null value") {
		if strings.Contains(errStr, "proposal_name") {
			return "Proposal name is required. Please provide a descriptive name for your cashflow proposal."
		}
		if strings.Contains(errStr, "description") {
			return "Item description is required. Please provide a clear description for each cashflow item."
		}
		if strings.Contains(errStr, "expected_amount") {
			return "Expected amount is required. Please specify the monetary amount for each cashflow item."
		}
		if strings.Contains(errStr, "cashflow_type") {
			return "Cashflow type (Inflow/Outflow) is required. Please specify whether this is money coming in or going out."
		}
		if strings.Contains(errStr, "category_id") {
			return "Category is required. Please select an appropriate category for each cashflow item."
		}
		if strings.Contains(errStr, "entity_name") {
			return "Entity name is required. Please specify which entity this cashflow belongs to."
		}
		if strings.Contains(errStr, "base_currency_code") {
			return "Base currency is required. Please specify the primary currency for this proposal."
		}
		if strings.Contains(errStr, "effective_date") {
			return "Effective date is required. Please specify when this cashflow proposal becomes active."
		}
		return "Required field missing. Please ensure all mandatory fields are filled out before submitting."
	}

	// Handle permission/access violations
	if strings.Contains(errStr, "permission denied") {
		return "You don't have permission to perform this action. Please contact your administrator to request access."
	}

	// Handle connection/timeout errors
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue. Please try again in a moment. If the problem persists, contact support."
	}

	// Return a more user-friendly version of the original error
	if strings.Contains(errStr, "constraint") {
		return "The data you entered violates a business rule. Please review your entries and ensure all fields meet the required criteria."
	}

	// Default fallback for unexpected errors
	return "Unable to save cashflow data due to a data validation issue. Please review your entries and try again. Error details: " + err.Error()
}
