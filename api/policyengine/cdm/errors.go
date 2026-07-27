package cdm

import (
	"errors"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgconn"
)

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func isAbortedTransaction(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "25P02"
}

// cdmConflictDetail turns a Postgres unique violation into a client-facing hint.
func cdmConflictDetail(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.ConstraintName != "" {
		switch pgErr.ConstraintName {
		case "cdm_variable_name_uq":
			return "CDM name already exists (including soft-deleted rows — pick a different evaluation key or restore the existing variable)"
		case "idx_cdm_variable_user_alias_uq":
			return "user_alias already in use by another active CDM variable"
		case "idx_cdm_variable_alias_name_uq":
			return "alias_name already in use by another CDM variable"
		}
		return "duplicate value: " + pgErr.ConstraintName
	}
	return "duplicate name, user_alias, or alias"
}

func respondCDMCreateError(w http.ResponseWriter, step string, err error) {
	if isUniqueViolation(err) {
		api.LogErrorForResponse(w, "cdm create %s: %v", step, err)
		api.RespondEnvelopeError(w, http.StatusConflict, "failed to create CDM variable ("+cdmConflictDetail(err)+")", "CDM_CREATE_FAILED")
		return
	}
	if isAbortedTransaction(err) {
		api.LogErrorForResponse(w, "cdm create %s: %v (transaction aborted — check prior step in trace log)", step, err)
		api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create CDM variable (transaction aborted — retry; contact support if this persists)", "CDM_CREATE_FAILED")
		return
	}
	api.LogErrorForResponse(w, "cdm create %s: %v", step, err)
	api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create CDM variable", "CDM_CREATE_FAILED")
}

func respondCDMUpdateError(w http.ResponseWriter, step string, err error) {
	if isUniqueViolation(err) {
		api.LogErrorForResponse(w, "cdm update %s: %v", step, err)
		api.RespondEnvelopeError(w, http.StatusConflict, "failed to update CDM variable ("+cdmConflictDetail(err)+")", "CDM_UPDATE_FAILED")
		return
	}
	if isAbortedTransaction(err) {
		api.LogErrorForResponse(w, "cdm update %s: %v", step, err)
		api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update CDM variable (transaction aborted)", "CDM_UPDATE_FAILED")
		return
	}
	msg := "failed to update CDM variable"
	if step == "audit" {
		msg = "failed to audit CDM update"
	}
	if strings.TrimSpace(step) == "" {
		step = "exec"
	}
	api.LogErrorForResponse(w, "cdm update %s: %v", step, err)
	api.RespondEnvelopeError(w, http.StatusInternalServerError, msg, "CDM_UPDATE_FAILED")
}
