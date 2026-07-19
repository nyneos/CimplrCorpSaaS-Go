package sweepconfig

import (
	"CimplrCorpSaas/api/constants"
	"fmt"
	"strings"
)

// parseSweepConstraintError converts PostgreSQL constraint violation errors into human-readable messages
func parseSweepConstraintError(err error) string {
	errStr := err.Error()

	// Unique constraint violations
	if strings.Contains(errStr, constants.ErrDuplicateKeyC) {
		if strings.Contains(errStr, "uniq_sweep_entity_bank_account") ||
			strings.Contains(errStr, "unique_sweep_configuration") {
			return "A sweep configuration already exists for this entity and bank account combination. Each entity can have only one sweep configuration per bank account."
		}
		if strings.Contains(errStr, "sweep_id") || strings.Contains(errStr, "primary") {
			return "This sweep ID already exists. Please try again with a different sweep ID."
		}
		return "This sweep configuration conflicts with an existing record. Please check for duplicate entity, bank, or account combinations."
	}

	// Foreign key constraint violations
	if strings.Contains(errStr, "violates foreign key constraint") {
		if strings.Contains(errStr, "fk_entity") {
			return "The specified entity does not exist. Please ensure the entity is properly configured before creating sweep rules."
		}
		if strings.Contains(errStr, "fk_bank") {
			return "The specified bank is not recognized. Please verify the bank name and try again."
		}
		if strings.Contains(errStr, "fk_parent_account") {
			return "The specified parent account does not exist or is not accessible. Please verify the parent account details."
		}
		return "One or more referenced records do not exist. Please verify all bank and account details."
	}

	// Check constraint violations
	if strings.Contains(errStr, "violates check constraint") {
		if strings.Contains(errStr, "chk_sweep_type") {
			return "Invalid sweep type. Please use a valid sweep type (e.g., 'AUTOMATIC', 'MANUAL', 'THRESHOLD')."
		}
		if strings.Contains(errStr, "chk_frequency") {
			return "Invalid frequency setting. Please specify a valid frequency (e.g., 'DAILY', 'WEEKLY', 'MONTHLY')."
		}
		if strings.Contains(errStr, "chk_buffer_amount") {
			return "Buffer amount must be a positive number or zero."
		}
		if strings.Contains(errStr, "chk_auto_sweep") {
			return "Auto sweep setting must be either 'Y' (Yes) or 'N' (No)."
		}
		if strings.Contains(errStr, "chk_active_status") {
			return "Active status must be either 'ACTIVE' or 'INACTIVE'."
		}
		return "One or more field values do not meet the required format or range constraints."
	}

	// NOT NULL constraint violations
	if strings.Contains(errStr, "null value in column") {
		if strings.Contains(errStr, "entity_name") {
			return "Entity name is required and cannot be empty."
		}
		if strings.Contains(errStr, "bank_name") {
			return "Bank name is required and cannot be empty."
		}
		if strings.Contains(errStr, "bank_account") {
			return "Bank account number is required and cannot be empty."
		}
		if strings.Contains(errStr, "sweep_type") {
			return "Sweep type is required. Please specify the type of sweep configuration."
		}
		if strings.Contains(errStr, "sweep_id") {
			return "Sweep ID is required for this operation."
		}
		return "A required field is missing. Please ensure all mandatory fields are provided."
	}

	// Permission/access violations
	if strings.Contains(errStr, "permission denied") {
		return "You do not have permission to create or modify sweep configurations for this entity."
	}

	// Connection/timeout errors
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue. Please try again in a moment."
	}

	// Default case - return a generic but helpful message
	return fmt.Sprintf("Unable to save sweep configuration due to a data constraint: %v", err)
}
