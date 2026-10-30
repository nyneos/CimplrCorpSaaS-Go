// Package errtext turns errors into text that is safe to put in a client response.
// Errors raised by our own validation keep their message. Database, network and
// context errors are logged in full against the response trace id and replaced by a
// short description, so driver detail (SQL, constraint names, hosts) never reaches the client.
package errtext

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Detail returns err's message when it is safe for the client, otherwise a short
// description after logging the full error for w's trace id.
func Detail(w http.ResponseWriter, err error) string {
	if err == nil {
		return ""
	}
	safe, internal := classify(err)
	if !internal {
		return err.Error()
	}
	api.LogErrorForResponse(w, "%v", err)
	return safe
}

// Text is Detail for code that has no response writer in hand, such as per-row
// results of a bulk request. Internal errors are logged without a trace id.
func Text(err error) string {
	if err == nil {
		return ""
	}
	safe, internal := classify(err)
	if !internal {
		return err.Error()
	}
	api.LogError("%v", err)
	return safe
}

func classify(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return "duplicate record", true
		case "23503":
			return "a referenced record was not found or is still in use", true
		case "23502", "23514", "22P02", "22003", "22007", "22008", "22001":
			return "invalid value", true
		case "40001", "40P01", "55P03":
			return "the record is busy, please retry", true
		}
		return "internal error", true
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "record not found", true
	}
	if errors.Is(err, pgx.ErrTxClosed) || errors.Is(err, pgx.ErrTxCommitRollback) {
		return "internal error", true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "request timed out", true
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return "internal error", true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return "internal error", true
	}
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) {
		return "malformed JSON", true
	}
	msg := err.Error()
	for _, sig := range driverSignatures {
		if strings.Contains(msg, sig) {
			return "internal error", true
		}
	}
	return "", false
}

// driverSignatures catch driver errors that were flattened with %v instead of wrapped.
var driverSignatures = []string{"(SQLSTATE ", "pq: ", "ERROR: ", "failed to connect to", "conn closed", "connection refused"}
