package limit

import (
	"CimplrCorpSaas/api/constants"
	"fmt"
	"strings"
)

// parseLimitConstraintError converts PostgreSQL constraint violation errors into human-readable messages
func parseLimitConstraintError(err error) string {
	errStr := err.Error()

	// Unique constraint violations
	if strings.Contains(errStr, constants.ErrDuplicateKeyC) {
		if strings.Contains(errStr, "uniq_bank_limit_full") ||
			strings.Contains(errStr, "unique_limit_combination") {
			return "A limit already exists for this entity, bank, and limit type combination. Each combination must be unique."
		}
		if strings.Contains(errStr, "uniq_bank_limit_utilization_full") ||
			strings.Contains(errStr, "unique_utilization_combination") {
			return "A utilization record already exists for this limit, date, and currency combination. Each utilization must be unique per day."
		}
		if strings.Contains(errStr, "limit_id") || strings.Contains(errStr, "primary") {
			return "This limit ID already exists. Please try again with a different limit ID."
		}
		if strings.Contains(errStr, "utilization_id") {
			return "This utilization ID already exists. Please try again with a different utilization ID."
		}
		return "This limit or utilization conflicts with an existing record. Please check for duplicate combinations."
	}

	// Foreign key constraint violations
	if strings.Contains(errStr, "violates foreign key constraint") {
		if strings.Contains(errStr, "fk_entity") {
			return "The specified entity does not exist. Please ensure the entity is properly configured before creating limits."
		}
		if strings.Contains(errStr, "fk_bank") {
			return "The specified bank is not recognized. Please verify the bank name and try again."
		}
		if strings.Contains(errStr, "fk_limit_id") {
			return "The specified limit does not exist. Please create the limit before recording utilization."
		}
		if strings.Contains(errStr, "fk_currency") {
			return "The specified currency code is not supported. Please use a valid currency code."
		}
		return "One or more referenced records do not exist. Please verify all entity, bank, and limit details."
	}

	// Check constraint violations
	if strings.Contains(errStr, "violates check constraint") {
		if strings.Contains(errStr, "chk_sanctioned_amount") {
			return "Sanctioned amount must be a positive number greater than zero."
		}
		if strings.Contains(errStr, "chk_utilized_amount") {
			return "Utilized amount must be a positive number or zero, and cannot exceed the sanctioned limit."
		}
		if strings.Contains(errStr, "chk_fungibility_pct") {
			return "Fungibility percentage must be between 0 and 100."
		}
		if strings.Contains(errStr, "chk_limit_type") {
			return "Invalid limit type. Please use a valid limit type (e.g., 'CREDIT', 'GUARANTEE', 'WORKING_CAPITAL')."
		}
		if strings.Contains(errStr, "chk_currency_code") {
			return "Invalid currency code. Please use a valid 3-letter currency code (e.g., 'USD', 'EUR', 'INR')."
		}
		if strings.Contains(errStr, "chk_effective_date") {
			return "Effective date cannot be in the future beyond allowed period."
		}
		if strings.Contains(errStr, "chk_utilization_date") {
			return "Utilization date cannot be in the future or before the limit effective date."
		}
		if strings.Contains(errStr, "chk_utilization_limit") {
			return "Total utilization cannot exceed the sanctioned limit amount."
		}
		return "One or more field values do not meet the required format, range, or business rule constraints."
	}

	// NOT NULL constraint violations
	if strings.Contains(errStr, "null value in column") {
		if strings.Contains(errStr, "entity_name") {
			return "Entity name is required and cannot be empty."
		}
		if strings.Contains(errStr, "bank_name") {
			return "Bank name is required and cannot be empty."
		}
		if strings.Contains(errStr, "core_limit_type") {
			return "Core limit type is required and cannot be empty."
		}
		if strings.Contains(errStr, "limit_type") {
			return "Limit type is required. Please specify the type of limit."
		}
		if strings.Contains(errStr, "currency_code") {
			return "Currency code is required for all limits and utilizations."
		}
		if strings.Contains(errStr, "sanctioned_amount") {
			return "Sanctioned amount is required for limit creation."
		}
		if strings.Contains(errStr, "utilized_amount") {
			return "Utilized amount is required for utilization records."
		}
		if strings.Contains(errStr, "utilization_date") {
			return "Utilization date is required for utilization records."
		}
		if strings.Contains(errStr, "limit_id") {
			return "Limit ID is required for this operation."
		}
		return "A required field is missing. Please ensure all mandatory fields are provided."
	}

	// Business rule violations (custom checks)
	if strings.Contains(errStr, "utilization exceeds limit") {
		return "The utilization amount would exceed the available limit. Please check the remaining headroom before proceeding."
	}

	if strings.Contains(errStr, "overlapping limit period") {
		return "This limit period overlaps with an existing limit for the same entity and bank. Please adjust the effective dates."
	}

	// Permission/access violations
	if strings.Contains(errStr, "permission denied") {
		return "You do not have permission to create or modify limits for this entity."
	}

	// Connection/timeout errors
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue. Please try again in a moment."
	}

	// Default case - return a generic but helpful message
	return fmt.Sprintf("Unable to save limit configuration due to a data constraint: %v", err)
}
