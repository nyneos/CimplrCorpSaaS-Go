package allMaster

import (
	"CimplrCorpSaas/api"
	middlewares "CimplrCorpSaas/api/middlewares"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlyCounterpartyError converts database errors to user-friendly messages
// Returns (error message, HTTP status code)
// Known/expected errors return 200 with error message, unexpected errors return 500/503
func getUserFriendlyCounterpartyError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	errStr := err.Error()

	// Duplicate counterparty name - Known error, return 200 for frontend to show message
	if strings.Contains(errStr, "unique_counterparty_name_not_deleted") {
		return "Counterparty name already exists. Please use a different name.", http.StatusOK
	}

	// Duplicate counterparty code - Known error, return 200
	if strings.Contains(errStr, "unique_counterparty_code_not_deleted") {
		return "Counterparty code already exists. Please use a different code.", http.StatusOK
	}

	// Generic duplicate key - Known error, return 200
	if strings.Contains(errStr, "duplicate key") {
		return "This counterparty already exists in the system.", http.StatusOK
	}

	// Foreign key violation - Known error, return 200
	if strings.Contains(errStr, "mastercounterpartybanks_counterparty_fkey") {
		return "Invalid counterparty. The counterparty does not exist.", http.StatusOK
	}
	
	// Generic foreign key violations - Known error, return 200
	if strings.Contains(errStr, "foreign key") || strings.Contains(errStr, "fkey") {
		return "Invalid reference. The related record does not exist.", http.StatusOK
	}

	// Check constraint violations - Known error, return 200
	if strings.Contains(errStr, "check constraint") {
		if strings.Contains(errStr, "actiontype_check") {
			return "Invalid action type. Must be CREATE, EDIT, or DELETE.", http.StatusOK
		}
		if strings.Contains(errStr, "processing_status_check") {
			return "Invalid processing status.", http.StatusOK
		}
		return "Invalid data provided. Please check your input.", http.StatusOK
	}

	// Not null violations - Known error, return 200
	if strings.Contains(errStr, "null value") || strings.Contains(errStr, "violates not-null") {
		if strings.Contains(errStr, "counterparty_name") {
			return "Counterparty name is required.", http.StatusOK
		}
		if strings.Contains(errStr, "counterparty_code") {
			return "Counterparty code is required.", http.StatusOK
		}
		if strings.Contains(errStr, "counterparty_type") {
			return "Counterparty type is required.", http.StatusOK
		}
		if strings.Contains(errStr, "country") {
			return "Country is required.", http.StatusOK
		}
		if strings.Contains(errStr, "currency") {
			return "Currency is required.", http.StatusOK
		}
		return "Required field is missing.", http.StatusOK
	}

	// Connection errors - SERVER ERROR (503 Service Unavailable)
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection error. Please try again.", http.StatusServiceUnavailable
	}

	// Return original error with context - SERVER ERROR (500)
	if context != "" {
		return context + ": " + errStr, http.StatusInternalServerError
	}
	return errStr, http.StatusInternalServerError
}

// Minimal request shape for counterparty create/sync
type CounterpartyRequest struct {
	InputMethod      string        `json:"input_method"`
	CounterpartyName string        `json:"counterparty_name"`
	CounterpartyCode string        `json:"counterparty_code"`
	CounterpartyType string        `json:"counterparty_type"`
	Address          string        `json:"address"`
	Status           string        `json:"status"`
	Country          string        `json:"country,omitempty"`
	Contact          string        `json:"contact,omitempty"`
	Email            string        `json:"email,omitempty"`
	EffFrom          string        `json:"eff_from,omitempty"`
	EffTo            string        `json:"eff_to,omitempty"`
	Tags             string        `json:"tags,omitempty"`
	Banks            []BankRequest `json:"banks,omitempty"`
}

type BankRequest struct {
	BankName string `json:"bank,omitempty"`
	Country  string `json:"country"`
	Branch   string `json:"branch,omitempty"`
	Account  string `json:"account,omitempty"`
	Swift    string `json:"swift,omitempty"`
	Rel      string `json:"rel,omitempty"`
	Currency string `json:"currency"`
	Category string `json:"category,omitempty"`
	Status   string `json:"status,omitempty"`
}

func normalizeHeader(headers []string) []string {
	out := make([]string, len(headers))
	for i, h := range headers {
		v := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(h, " ", "_")))
		v = strings.Trim(v, "\"',")
		out[i] = v
	}
	return out
}

// CreateCounterparties inserts rows into mastercounterparty and creates audit entries
func CreateCounterparties(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string                `json:"user_id"`
			Rows   []CounterpartyRequest `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		createdBy := session.Name

		ctx := r.Context()
		created := make([]map[string]interface{}, 0)

		for _, rrow := range req.Rows {
			if rrow.CounterpartyName == "" || rrow.CounterpartyCode == "" || rrow.CounterpartyType == "" {
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "missing required fields", "counterparty_name": rrow.CounterpartyName})
				continue
			}

			// Validate banks before transaction
			shouldSkip := false
			for _, b := range rrow.Banks {
				if strings.TrimSpace(b.BankName) != "" && !api.IsBankAllowed(ctx, b.BankName) {
					created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized bank: " + b.BankName, "counterparty_name": rrow.CounterpartyName})
					shouldSkip = true
					break
				}
				if strings.TrimSpace(b.Currency) != "" && !api.IsCurrencyAllowed(ctx, b.Currency) {
					created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized currency: " + b.Currency, "counterparty_name": rrow.CounterpartyName})
					shouldSkip = true
					break
				}
				if strings.TrimSpace(b.Category) != "" && !api.IsCashFlowCategoryAllowed(ctx, b.Category) {
					created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized category: " + b.Category, "counterparty_name": rrow.CounterpartyName})
					shouldSkip = true
					break
				}
			}
			if shouldSkip {
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "failed to start tx: " + err.Error()})
				continue
			}

			var id string
			// parse eff_from/eff_to
			var effFrom, effTo interface{}
			if strings.TrimSpace(rrow.EffFrom) != "" {
				if norm := NormalizeDate(rrow.EffFrom); norm != "" {
					if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
						effFrom = tval
					}
				}
			}
			if strings.TrimSpace(rrow.EffTo) != "" {
				if norm := NormalizeDate(rrow.EffTo); norm != "" {
					if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
						effTo = tval
					}
				}
			}

			// generate counterparty_id explicitly with CPT- prefix
			id = "CPT-" + strings.ToUpper(strings.ReplaceAll(uuid.New().String(), "-", ""))[:7]
			q := `INSERT INTO mastercounterparty (counterparty_id, input_method, counterparty_name, counterparty_code, counterparty_type, address, status, country, contact, email, eff_from, eff_to, tags)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`
			if _, err := tx.Exec(ctx, q, id, rrow.InputMethod, rrow.CounterpartyName, rrow.CounterpartyCode, rrow.CounterpartyType, rrow.Address, rrow.Status, rrow.Country, rrow.Contact, rrow.Email, effFrom, effTo, rrow.Tags); err != nil {
				tx.Rollback(ctx)
				errMsg, _ := getUserFriendlyCounterpartyError(err, "Failed to create counterparty")
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: errMsg, "counterparty_name": rrow.CounterpartyName})
				continue
			}

			// If banks are provided, insert them into mastercounterpartybanks within same transaction
			var bankResults []map[string]interface{}
			for _, b := range rrow.Banks {
				// required: bank (name), country, currency
				if strings.TrimSpace(b.BankName) == "" || strings.TrimSpace(b.Country) == "" || strings.TrimSpace(b.Currency) == "" {
					bankResults = append(bankResults, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "missing bank required fields"})
					continue
				}
				// generate bank_id with CBn- prefix
				bankID := "CBn-" + strings.ToUpper(strings.ReplaceAll(uuid.New().String(), "-", ""))[:7]

				// prepare nullable values
				var branch interface{}
				if strings.TrimSpace(b.Branch) != "" {
					branch = b.Branch
				}
				var account interface{}
				if strings.TrimSpace(b.Account) != "" {
					account = b.Account
				}
				var swift interface{}
				if strings.TrimSpace(b.Swift) != "" {
					swift = b.Swift
				}
				var rel interface{}
				if strings.TrimSpace(b.Rel) != "" {
					rel = b.Rel
				}
				var category interface{}
				if strings.TrimSpace(b.Category) != "" {
					category = b.Category
				}
				status := b.Status
				if strings.TrimSpace(status) == "" {
					status = "Active"
				}

				bankQ := `INSERT INTO mastercounterpartybanks (bank_id, counterparty_id, bank, country, branch, account, swift, rel, currency, category, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`
				if _, err := tx.Exec(ctx, bankQ, bankID, id, b.BankName, b.Country, branch, account, swift, rel, b.Currency, category, status); err != nil {
					tx.Rollback(ctx)
					errMsg, _ := getUserFriendlyCounterpartyError(err, "Bank information error")
					created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: errMsg, "counterparty_name": rrow.CounterpartyName})
					goto nextRow
				}
				bankResults = append(bankResults, map[string]interface{}{constants.ValueSuccess: true, "bank_id": bankID})
			}
		nextRow:

			auditQ := `INSERT INTO auditactioncounterparty (counterparty_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, auditQ, id, nil, createdBy); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrAuditInsertFailed + err.Error(), "id": id})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailed + err.Error(), "id": id})
				continue
			}
			entry := map[string]interface{}{constants.ValueSuccess: true, "counterparty_id": id, "counterparty_name": rrow.CounterpartyName}
			if len(bankResults) > 0 {
				entry["banks"] = bankResults
			}
			created = append(created, entry)
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(created), "", created)
	}
}

type cpAuditInfo struct {
	CreatedBy string
	CreatedAt string
	EditedBy  string
	EditedAt  string
	DeletedBy string
	DeletedAt string
}

// GetCounterpartyNamesWithID returns rows with audit info
func GetCounterpartyNamesWithID(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()

		mainQ := `
			WITH latest AS (
				SELECT DISTINCT ON (counterparty_id) counterparty_id, processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason
				FROM auditactioncounterparty
				ORDER BY counterparty_id, requested_at DESC
			)
	     SELECT m.counterparty_id, m.input_method, m.counterparty_name, m.old_counterparty_name, m.counterparty_code, m.old_counterparty_code, m.counterparty_type, m.old_counterparty_type, m.address, m.old_address, m.status, m.old_status, m.country, m.old_country, m.contact, m.old_contact, m.email, m.old_email, m.eff_from, m.old_eff_from, m.eff_to, m.old_eff_to, m.tags, m.old_tags, m.is_deleted,
				   l.processing_status, l.requested_by, l.requested_at, l.actiontype, l.action_id, l.checker_by, l.checker_at, l.checker_comment, l.reason
			FROM mastercounterparty m
			LEFT JOIN latest l ON l.counterparty_id = m.counterparty_id
			ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC
		`

		rows, err := pgxPool.Query(ctx, mainQ)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		order := make([]string, 0)
		outMap := make(map[string]map[string]interface{})

		for rows.Next() {
			var (
				id               string
				inputMethod      interface{}
				name             interface{}
				oldName          interface{}
				code             interface{}
				oldCode          interface{}
				ctype            interface{}
				oldCtype         interface{}
				addr             interface{}
				oldAddr          interface{}
				status           interface{}
				oldStatus        interface{}
				country          interface{}
				oldCountry       interface{}
				contact          interface{}
				oldContact       interface{}
				email            interface{}
				oldEmail         interface{}
				effFrom          interface{}
				oldEffFrom       interface{}
				effTo            interface{}
				oldEffTo         interface{}
				tags             interface{}
				oldTags          interface{}
				processingStatus interface{}
				requestedBy      interface{}
				requestedAt      interface{}
				actionType       interface{}
				actionID         string
				checkerBy        interface{}
				checkerAt        interface{}
				checkerComment   interface{}
				reason           interface{}
				isDeleted        bool
			)

			if err := rows.Scan(&id, &inputMethod, &name, &oldName, &code, &oldCode, &ctype, &oldCtype, &addr, &oldAddr, &status, &oldStatus, &country, &oldCountry, &contact, &oldContact, &email, &oldEmail, &effFrom, &oldEffFrom, &effTo, &oldEffTo, &tags, &oldTags, &isDeleted, &processingStatus, &requestedBy, &requestedAt, &actionType, &actionID, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
				continue
			}

			// If record is marked deleted and latest processing is APPROVED, skip it (soft-delete)
			if isDeleted {
				if ps := strings.ToUpper(strings.TrimSpace(ifaceToString(processingStatus))); ps == "APPROVED" {
					continue
				}
			}

			order = append(order, id)
			outMap[id] = map[string]interface{}{
				"counterparty_id":       id,
				"input_method":          ifaceToString(inputMethod),
				"counterparty_name":     ifaceToString(name),
				"old_counterparty_name": ifaceToString(oldName),
				"counterparty_code":     ifaceToString(code),
				"old_counterparty_code": ifaceToString(oldCode),
				"counterparty_type":     ifaceToString(ctype),
				"old_counterparty_type": ifaceToString(oldCtype),
				"address":               ifaceToString(addr),
				"old_address":           ifaceToString(oldAddr),
				constants.KeyStatus:     ifaceToString(status),
				"old_status":            ifaceToString(oldStatus),
				"country":               ifaceToString(country),
				"old_country":           ifaceToString(oldCountry),
				"contact":               ifaceToString(contact),
				"old_contact":           ifaceToString(oldContact),
				"email":                 ifaceToString(email),
				"old_email":             ifaceToString(oldEmail),
				"eff_from":              ifaceToTimeString(effFrom),
				"old_eff_from":          ifaceToTimeString(oldEffFrom),
				"eff_to":                ifaceToTimeString(effTo),
				"old_eff_to":            ifaceToTimeString(oldEffTo),
				"tags":                  ifaceToString(tags),
				"old_tags":              ifaceToString(oldTags),
				"processing_status":     ifaceToString(processingStatus),
				"requested_by":          ifaceToString(requestedBy),
				"requested_at":          ifaceToTimeString(requestedAt),
				"action_type":           ifaceToString(actionType),
				"action_id":             actionID,
				"checker_by":            ifaceToString(checkerBy),
				"checker_at":            ifaceToTimeString(checkerAt),
				"checker_comment":       ifaceToString(checkerComment),
				"reason":                ifaceToString(reason),
				"created_by":            "", "created_at": "", "edited_by": "", "edited_at": "", "deleted_by": "", "deleted_at": "",
			}
		}

		if len(order) > 0 {
			auditQ := `SELECT counterparty_id, actiontype, requested_by, requested_at FROM auditactioncounterparty WHERE counterparty_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY counterparty_id, requested_at DESC`
			arows, aerr := pgxPool.Query(ctx, auditQ, order)
			if aerr == nil {
				defer arows.Close()
				auditMap := make(map[string]*cpAuditInfo)
				for arows.Next() {
					var tid, atype string
					var rby, rat interface{}
					if err := arows.Scan(&tid, &atype, &rby, &rat); err != nil {
						continue
					}
					if auditMap[tid] == nil {
						auditMap[tid] = &cpAuditInfo{}
					}
					switch strings.ToUpper(strings.TrimSpace(atype)) {
					case "CREATE":
						if auditMap[tid].CreatedBy == "" {
							auditMap[tid].CreatedBy = ifaceToString(rby)
							auditMap[tid].CreatedAt = ifaceToTimeString(rat)
						}
					case "EDIT":
						if auditMap[tid].EditedBy == "" {
							auditMap[tid].EditedBy = ifaceToString(rby)
							auditMap[tid].EditedAt = ifaceToTimeString(rat)
						}
					case "DELETE":
						if auditMap[tid].DeletedBy == "" {
							auditMap[tid].DeletedBy = ifaceToString(rby)
							auditMap[tid].DeletedAt = ifaceToTimeString(rat)
						}
					}
				}
				for _, tid := range order {
					if a, ok := auditMap[tid]; ok {
						if rec, ok2 := outMap[tid]; ok2 {
							rec["created_by"] = a.CreatedBy
							rec["created_at"] = a.CreatedAt
							rec["edited_by"] = a.EditedBy
							rec["edited_at"] = a.EditedAt
							rec["deleted_by"] = a.DeletedBy
							rec["deleted_at"] = a.DeletedAt
							outMap[tid] = rec
						}
					}
				}
			}
		}

		out := make([]map[string]interface{}, 0, len(order))
		for _, tid := range order {
			if rec, ok := outMap[tid]; ok {
				out = append(out, rec)
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
	}
}

// GetCounterpartyBanks returns bank rows for a given counterparty_id
func GetCounterpartyBanks(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string `json:"user_id"`
			CounterpartyID string `json:"counterparty_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing counterparty_id")
			return
		}
		if req.CounterpartyID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_id required")
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()

		bankNames := api.GetBankNamesFromCtx(ctx)
		currCodes := api.GetCurrencyCodesFromCtx(ctx)
		categories := api.GetCashFlowCategoryNamesFromCtx(ctx)

		baseQ := `SELECT bank_id, bank, old_bank, country, old_country, branch, old_branch, account, old_account, swift, old_swift, rel, old_rel, currency, old_currency, category, old_category, status, old_status FROM mastercounterpartybanks WHERE counterparty_id = $1`

		filters := []string{}
		args := []interface{}{req.CounterpartyID}
		argIdx := 2

		if len(bankNames) > 0 {
			filters = append(filters, fmt.Sprintf("(bank IS NULL OR bank = ANY($%d))", argIdx))
			args = append(args, bankNames)
			argIdx++
		}
		if len(currCodes) > 0 {
			filters = append(filters, fmt.Sprintf("(currency IS NULL OR currency = ANY($%d))", argIdx))
			args = append(args, currCodes)
			argIdx++
		}
		if len(categories) > 0 {
			filters = append(filters, fmt.Sprintf("(category IS NULL OR category = ANY($%d))", argIdx))
			args = append(args, categories)
		}

		if len(filters) > 0 {
			baseQ += " AND " + strings.Join(filters, " AND ")
		}

		rows, err := pgxPool.Query(ctx, baseQ, args...)
		if err != nil {
			errMsg, statusCode := getUserFriendlyCounterpartyError(err, "Query failed")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var bankID string
			var bank, oldBank, country, oldCountry, branch, oldBranch, account, oldAccount, swift, oldSwift, rel, oldRel, currency, oldCurrency, category, oldCategory, status, oldStatus interface{}
			if err := rows.Scan(&bankID, &bank, &oldBank, &country, &oldCountry, &branch, &oldBranch, &account, &oldAccount, &swift, &oldSwift, &rel, &oldRel, &currency, &oldCurrency, &category, &oldCategory, &status, &oldStatus); err != nil {
				continue
			}
			out = append(out, map[string]interface{}{
				"bank_id":           bankID,
				"bank":              ifaceToString(bank),
				"old_bank":          ifaceToString(oldBank),
				"country":           ifaceToString(country),
				"old_country":       ifaceToString(oldCountry),
				"branch":            ifaceToString(branch),
				"old_branch":        ifaceToString(oldBranch),
				"account":           ifaceToString(account),
				"old_account":       ifaceToString(oldAccount),
				"swift":             ifaceToString(swift),
				"old_swift":         ifaceToString(oldSwift),
				"rel":               ifaceToString(rel),
				"old_rel":           ifaceToString(oldRel),
				"currency":          ifaceToString(currency),
				"old_currency":      ifaceToString(oldCurrency),
				"category":          ifaceToString(category),
				"old_category":      ifaceToString(oldCategory),
				constants.KeyStatus: ifaceToString(status),
				"old_status":        ifaceToString(oldStatus),
			})
		}
		if err := rows.Err(); err != nil {
			errMsg, statusCode := getUserFriendlyCounterpartyError(err, "Failed to process data")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// GetApprovedActiveCounterparties returns minimal rows where latest audit is APPROVED and master status is Active
func GetApprovedActiveCounterparties(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		q := `
			WITH latest AS (
				SELECT DISTINCT ON (counterparty_id) counterparty_id, processing_status
				FROM auditactioncounterparty
				ORDER BY counterparty_id, requested_at DESC
			)
			SELECT m.counterparty_id, m.counterparty_name, m.counterparty_code, m.counterparty_type
			FROM mastercounterparty m
			JOIN latest l ON l.counterparty_id = m.counterparty_id
			WHERE UPPER(l.processing_status) = 'APPROVED' AND UPPER(m.status) = 'ACTIVE'
		`
		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var id string
			var name, code, ctype interface{}
			if err := rows.Scan(&id, &name, &code, &ctype); err != nil {
				continue
			}
			out = append(out, map[string]interface{}{"counterparty_id": id, "counterparty_name": ifaceToString(name), "counterparty_code": ifaceToString(code), "counterparty_type": ifaceToString(ctype)})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
	}
}

// UpdateCounterpartyBulk updates multiple counterparties and creates edit audit actions
func UpdateCounterpartyBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				CounterpartyID string                 `json:"counterparty_id"`
				Fields         map[string]interface{} `json:"fields"`
				Reason         string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		updatedBy := session.Name

		ctx := r.Context()
		results := []map[string]interface{}{}
		for _, row := range req.Rows {
			if row.CounterpartyID == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "missing counterparty_id"})
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "begin failed: " + err.Error()})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
				}()
				sel := `SELECT counterparty_name, counterparty_code, counterparty_type, address, status, country, contact, email, eff_from, eff_to, tags FROM mastercounterparty WHERE counterparty_id=$1 FOR UPDATE`
				var existingName, existingCode, existingType, existingAddr, existingStatus, existingCountry, existingContact, existingEmail, existingEffFrom, existingEffTo, existingTags interface{}
				if err := tx.QueryRow(ctx, sel, row.CounterpartyID).Scan(&existingName, &existingCode, &existingType, &existingAddr, &existingStatus, &existingCountry, &existingContact, &existingEmail, &existingEffFrom, &existingEffTo, &existingTags); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "fetch failed: " + err.Error(), "counterparty_id": row.CounterpartyID})
					return
				}

				var sets []string
				var args []interface{}
				pos := 1
				for k, v := range row.Fields {
					switch k {
					case "counterparty_name":
						sets = append(sets, fmt.Sprintf("counterparty_name=$%d, old_counterparty_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingName))
						pos += 2
					case "counterparty_code":
						sets = append(sets, fmt.Sprintf("counterparty_code=$%d, old_counterparty_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCode))
						pos += 2
					case "counterparty_type":
						sets = append(sets, fmt.Sprintf("counterparty_type=$%d, old_counterparty_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingType))
						pos += 2
					case "address":
						sets = append(sets, fmt.Sprintf("address=$%d, old_address=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingAddr))
						pos += 2
					case constants.KeyStatus:
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingStatus))
						pos += 2
					case "country":
						sets = append(sets, fmt.Sprintf("country=$%d, old_country=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCountry))
						pos += 2
					case "contact":
						sets = append(sets, fmt.Sprintf("contact=$%d, old_contact=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingContact))
						pos += 2
					case "email":
						sets = append(sets, fmt.Sprintf("email=$%d, old_email=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingEmail))
						pos += 2
					case "eff_from":
						// parse date and set old_eff_from
						if s := strings.TrimSpace(fmt.Sprint(v)); s == "" {
							sets = append(sets, fmt.Sprintf("eff_from=$%d, old_eff_from=$%d", pos, pos+1))
							args = append(args, nil, existingEffFrom)
							pos += 2
						} else {
							if norm := NormalizeDate(s); norm != "" {
								if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
									sets = append(sets, fmt.Sprintf("eff_from=$%d, old_eff_from=$%d", pos, pos+1))
									args = append(args, tval, existingEffFrom)
									pos += 2
								}
							}
						}
					case "eff_to":
						if s := strings.TrimSpace(fmt.Sprint(v)); s == "" {
							sets = append(sets, fmt.Sprintf("eff_to=$%d, old_eff_to=$%d", pos, pos+1))
							args = append(args, nil, existingEffTo)
							pos += 2
						} else {
							if norm := NormalizeDate(s); norm != "" {
								if tval, err := time.Parse(constants.DateFormat, norm); err == nil {
									sets = append(sets, fmt.Sprintf("eff_to=$%d, old_eff_to=$%d", pos, pos+1))
									args = append(args, tval, existingEffTo)
									pos += 2
								}
							}
						}
					case "tags":
						sets = append(sets, fmt.Sprintf("tags=$%d, old_tags=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTags))
						pos += 2
					default:
						// ignore other fields for now
					}
				}

				updatedID := row.CounterpartyID
				if len(sets) > 0 {
					q := "UPDATE mastercounterparty SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE counterparty_id=$%d RETURNING counterparty_id", pos)
					args = append(args, row.CounterpartyID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedID); err != nil {
						errMsg, _ := getUserFriendlyCounterpartyError(err, "Update failed")
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: errMsg, "counterparty_id": row.CounterpartyID})
						return
					}
				}

				auditQ := `INSERT INTO auditactioncounterparty (counterparty_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL', $2, $3, now())`
				if _, err := tx.Exec(ctx, auditQ, updatedID, row.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "audit failed: " + err.Error(), "counterparty_id": updatedID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailed + err.Error(), "counterparty_id": updatedID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "counterparty_id": updatedID})
			}()
		}

		overall := true
		for _, r := range results {
			if ok, exists := r[constants.ValueSuccess]; exists {
				if b, okb := ok.(bool); okb {
					if !b {
						overall = false
						break
					}
				} else {
					overall = false
					break
				}
			} else {
				overall = false
				break
			}
		}
		api.RespondWithPayload(w, overall, "", results)
	}
}

// UpdateCounterpartyBanksBulk updates multiple counterpartybanks by bank_id
// and inserts an EDIT audit action for the parent counterparty to mark
// the counterparty processing status as pending edit approval.
func UpdateCounterpartyBanksBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				BankID string                 `json:"bank_id"`
				Fields map[string]interface{} `json:"fields"`
				Reason string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		updatedBy := session.Name

		ctx := r.Context()
		results := []map[string]interface{}{}
		for _, row := range req.Rows {
			if strings.TrimSpace(row.BankID) == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "missing bank_id"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "begin failed: " + err.Error(), "bank_id": row.BankID})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
				}()

				// lock existing bank row and fetch counterparty_id
				sel := `SELECT counterparty_id, bank, country, branch, account, swift, rel, currency, category, status FROM mastercounterpartybanks WHERE bank_id=$1 FOR UPDATE`
				var counterpartyID string
				var exBank, exCountry, exBranch, exAccount, exSwift, exRel, exCurrency, exCategory, exStatus interface{}
				if err := tx.QueryRow(ctx, sel, row.BankID).Scan(&counterpartyID, &exBank, &exCountry, &exBranch, &exAccount, &exSwift, &exRel, &exCurrency, &exCategory, &exStatus); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "fetch failed: " + err.Error(), "bank_id": row.BankID})
					return
				}

				var sets []string
				var args []interface{}
				pos := 1
				for k, v := range row.Fields {
					switch k {
					case "bank":
						sets = append(sets, fmt.Sprintf("bank=$%d, old_bank=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(exBank))
						pos += 2
					case "country":
						sets = append(sets, fmt.Sprintf("country=$%d, old_country=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(exCountry))
						pos += 2
					case "branch":
						sets = append(sets, fmt.Sprintf("branch=$%d, old_branch=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(exBranch))
						pos += 2
					case "account":
						sets = append(sets, fmt.Sprintf("account=$%d, old_account=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(exAccount))
						pos += 2
					case "swift":
						sets = append(sets, fmt.Sprintf("swift=$%d, old_swift=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(exSwift))
						pos += 2
					case "rel":
						sets = append(sets, fmt.Sprintf("rel=$%d, old_rel=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(exRel))
						pos += 2
					case "currency":
						sets = append(sets, fmt.Sprintf("currency=$%d, old_currency=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(exCurrency))
						pos += 2
					case "category":
						sets = append(sets, fmt.Sprintf("category=$%d, old_category=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(exCategory))
						pos += 2
					case constants.KeyStatus:
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(exStatus))
						pos += 2
					default:
						// ignore unknown fields
					}
				}

				updatedBankID := row.BankID
				if len(sets) > 0 {
					q := "UPDATE mastercounterpartybanks SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE bank_id=$%d RETURNING bank_id", pos)
					args = append(args, row.BankID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedBankID); err != nil {
						errMsg, _ := getUserFriendlyCounterpartyError(err, "Bank update failed")
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: errMsg, "bank_id": row.BankID})
						return
					}
				}

				// insert audit action for parent counterparty to mark edit pending
				auditQ := `INSERT INTO auditactioncounterparty (counterparty_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL', $2, $3, now())`
				if _, err := tx.Exec(ctx, auditQ, counterpartyID, row.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrAuditInsertFailed + err.Error(), "bank_id": updatedBankID, "counterparty_id": counterpartyID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailed + err.Error(), "bank_id": updatedBankID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "bank_id": updatedBankID, "counterparty_id": counterpartyID})
			}()
		}

		finalSuccess := api.IsBulkSuccess(results)
		api.RespondWithPayload(w, finalSuccess, "", results)
	}
}

// DeleteCounterparty inserts a DELETE audit action (no hard delete)
func DeleteCounterparty(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// accept bulk delete: counterparty_ids
		var body struct {
			UserID          string   `json:"user_id"`
			CounterpartyIDs []string `json:"counterparty_ids"`
			Reason          string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		requestedBy := session.Name

		if len(body.CounterpartyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_ids required")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		q := `INSERT INTO auditactioncounterparty (counterparty_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`
		for _, id := range body.CounterpartyIDs {
			if _, err := tx.Exec(ctx, q, id, body.Reason, requestedBy); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}
		committed = true

		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "counterparty_ids": body.CounterpartyIDs})
	}
}

// BulkRejectCounterpartyActions rejects latest audit actions for counterparty_ids
func BulkRejectCounterpartyActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			CounterpartyIDs []string `json:"counterparty_ids"`
			Comment         string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		checkerBy := session.Name

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		sel := `SELECT DISTINCT ON (counterparty_id) action_id, counterparty_id, processing_status FROM auditactioncounterparty WHERE counterparty_id = ANY($1) ORDER BY counterparty_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.CounterpartyIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch audit rows: "+err.Error())
			return
		}
		defer rows.Close()
		actionIDs := make([]string, 0)
		found := map[string]bool{}
		var cannotReject []string
		for rows.Next() {
			var aid, cid, pstatus string
			if err := rows.Scan(&aid, &cid, &pstatus); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to scan audit rows: "+err.Error())
				return
			}
			found[cid] = true
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotReject = append(cannotReject, cid)
			}
			actionIDs = append(actionIDs, aid)
		}
		missing := []string{}
		for _, cid := range req.CounterpartyIDs {
			if !found[cid] {
				missing = append(missing, cid)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for counterparty_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved counterparty_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		upd := `UPDATE auditactioncounterparty SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id, counterparty_id`
		urows, err := tx.Query(ctx, upd, checkerBy, req.Comment, actionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to update audit rows: "+err.Error())
			return
		}
		defer urows.Close()
		updated := []map[string]interface{}{}
		for urows.Next() {
			var aid, cid string
			if err := urows.Scan(&aid, &cid); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": aid, "counterparty_id": cid})
			}
		}
		if len(updated) != len(actionIDs) {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("updated %d of %d actions, aborting", len(updated), len(actionIDs)))
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}
		committed = true
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "updated": updated})
	}
}

// BulkApproveCounterpartyActions approves latest audit actions for counterparty_ids
func BulkApproveCounterpartyActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			CounterpartyIDs []string `json:"counterparty_ids"`
			Comment         string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		checkerBy := session.Name

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		sel := `SELECT DISTINCT ON (counterparty_id) action_id, counterparty_id, actiontype, processing_status FROM auditactioncounterparty WHERE counterparty_id = ANY($1) ORDER BY counterparty_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.CounterpartyIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch audit rows: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		foundTypes := map[string]bool{}
		cannotApprove := []string{}
		actionTypeByID := map[string]string{}
		for rows.Next() {
			var aid, cid, atype, pstatus string
			if err := rows.Scan(&aid, &cid, &atype, &pstatus); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to scan audit rows: "+err.Error())
				return
			}
			foundTypes[cid] = true
			actionIDs = append(actionIDs, aid)
			actionTypeByID[cid] = atype
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotApprove = append(cannotApprove, cid)
			}
		}
		missing := []string{}
		for _, cid := range req.CounterpartyIDs {
			if !foundTypes[cid] {
				missing = append(missing, cid)
			}
		}
		if len(missing) > 0 || len(cannotApprove) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for counterparty_ids: %v. ", missing)
			}
			if len(cannotApprove) > 0 {
				msg += fmt.Sprintf("cannot approve already approved counterparty_ids: %v", cannotApprove)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		upd := `UPDATE auditactioncounterparty SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id, counterparty_id, actiontype`
		urows, err := tx.Query(ctx, upd, checkerBy, req.Comment, actionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to update audit rows: "+err.Error())
			return
		}
		defer urows.Close()
		updated := []map[string]interface{}{}
		deleteIDs := make([]string, 0)
		for urows.Next() {
			var aid, cid, atype string
			if err := urows.Scan(&aid, &cid, &atype); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": aid, "counterparty_id": cid, "action_type": atype})
				if strings.ToUpper(strings.TrimSpace(atype)) == "DELETE" {
					deleteIDs = append(deleteIDs, cid)
				}
			}
		}
		if len(updated) != len(actionIDs) {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("updated %d of %d actions, aborting", len(updated), len(actionIDs)))
			return
		}
		if len(deleteIDs) > 0 {
			// perform soft delete by setting is_deleted = true
			delQ := `UPDATE mastercounterparty SET is_deleted = true WHERE counterparty_id = ANY($1)`
			if _, err := tx.Exec(ctx, delQ, deleteIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to soft-delete master rows: "+err.Error())
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}
		committed = true
		api.RespondWithPayload(w, true, "", updated)
	}
}

// UploadCounterparty handles staging and mapping for mastercounterparty
func UploadCounterparty(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := ""
		if r.Header.Get(constants.ContentTypeText) == constants.ContentTypeJSON {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in body")
				return
			}
		} else {
			userID = r.FormValue(constants.KeyUserID)
			if userID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in form")
				return
			}
		}
		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		userName := session.Name

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseMultipartForm)
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFilesUploaded)
			return
		}
		batchIDs := make([]string, 0, len(files))
		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file: "+fh.Filename)
				return
			}
			ext := getFileExt(fh.Filename)
			records, err := parseCashFlowCategoryFile(f, ext)
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file: "+fh.Filename)
				return
			}
			headerRow := records[0]
			dataRows := records[1:]
			batchID := uuid.New().String()
			batchIDs = append(batchIDs, batchID)
			colCount := len(headerRow)
			copyRows := make([][]interface{}, len(dataRows))
			for i, row := range dataRows {
				vals := make([]interface{}, colCount+1)
				vals[0] = batchID
				for j := 0; j < colCount; j++ {
					if j < len(row) {
						cell := strings.TrimSpace(row[j])
						if cell == "" {
							vals[j+1] = nil
						} else {
							vals[j+1] = cell
						}
					} else {
						vals[j+1] = nil
					}
				}
				copyRows[i] = vals
			}
			headerNorm := make([]string, len(headerRow))
			for i, h := range headerRow {
				hn := strings.TrimSpace(h)
				// remove stray trailing commas or punctuation from CSV headers
				hn = strings.Trim(hn, ", ")
				hn = strings.ToLower(hn)
				hn = strings.ReplaceAll(hn, " ", "_")
				hn = strings.Trim(hn, "\"'`")
				headerNorm[i] = hn
			}
			columns := append([]string{"upload_batch_id"}, headerNorm...)

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()

			_, err = tx.CopyFrom(ctx, pgx.Identifier{"input_counterparty_table"}, columns, pgx.CopyFromRows(copyRows))
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}

			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_counterparty`)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Mapping error")
				return
			}
			mapping := make(map[string]string)
			for mapRows.Next() {
				var src, tgt string
				if err := mapRows.Scan(&src, &tgt); err == nil {
					key := strings.ToLower(strings.TrimSpace(src))
					key = strings.ReplaceAll(key, " ", "_")
					// sanitize target field name: trim spaces, trailing commas and quotes
					tt := strings.TrimSpace(tgt)
					tt = strings.Trim(tt, ", \"'`")
					tt = strings.ReplaceAll(tt, " ", "_")
					mapping[key] = tt
				}
			}
			mapRows.Close()
			var srcCols []string
			var tgtCols []string
			for i, h := range headerRow {
				key := headerNorm[i]
				if t, ok := mapping[key]; ok {
					srcCols = append(srcCols, key)
					tgtCols = append(tgtCols, t)
				} else {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf(constants.ErrNoMappingForSourceColumn, h))
					return
				}
			}
			tgtColsStr := strings.Join(tgtCols, ", ")
			var selectExprs []string
			for i, src := range srcCols {
				tgt := tgtCols[i]
				selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", src, tgt))
			}
			srcColsStr := strings.Join(selectExprs, ", ")

			insertSQL := fmt.Sprintf(`INSERT INTO mastercounterparty (%s) SELECT %s FROM input_counterparty_table s WHERE s.upload_batch_id = $1 RETURNING counterparty_id`, tgtColsStr, srcColsStr)
			rows, err := tx.Query(ctx, insertSQL, batchID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Final insert error: "+err.Error())
				return
			}
			var newIDs []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err == nil {
					newIDs = append(newIDs, id)
				}
			}
			rows.Close()
			if len(newIDs) > 0 {
				auditSQL := `INSERT INTO auditactioncounterparty (counterparty_id, actiontype, processing_status, reason, requested_by, requested_at) SELECT counterparty_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now() FROM mastercounterparty WHERE counterparty_id = ANY($2)`
				if _, err := tx.Exec(ctx, auditSQL, userName, newIDs); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit actions: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
				return
			}
			committed = true
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "batch_ids": batchIDs})
	}
}

func UploadCounterpartySimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// === Step 1: Identify user ===
		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			var req struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			userID = req.UserID
		}
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		userName := session.Name

		// === Step 2: Parse uploaded CSV ===
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseForm+err.Error())
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No file uploaded")
			return
		}

		// === Step 3: Allowed fields ===
		allowed := map[string]bool{
			"counterparty_name": true,
			"counterparty_code": true,
			"counterparty_type": true,
			"address":           true,
			constants.KeyStatus: true,
			"country":           true,
			"contact":           true,
			"email":             true,
			"eff_from":          true,
			"eff_to":            true,
			"tags":              true,
		}

		batchIDs := []string{}

		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToOpenFile)
				return
			}
			records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty CSV file")
				return
			}

			// === Step 4: Normalize headers ===
			headers := normalizeHeader(records[0])
			dataRows := records[1:]

			validCols := []string{}
			for _, h := range headers {
				if allowed[h] {
					validCols = append(validCols, h)
				}
			}

			// Required columns check
			if !(slices.Contains(validCols, "counterparty_code") &&
				slices.Contains(validCols, "counterparty_name") &&
				slices.Contains(validCols, "counterparty_type")) {
				api.RespondWithError(w, http.StatusBadRequest,
					"CSV must include counterparty_code, counterparty_name, counterparty_type")
				return
			}

			headerPos := map[string]int{}
			for i, h := range headers {
				headerPos[h] = i
			}

			copyRows := make([][]interface{}, len(dataRows))
			counterpartyCodes := make([]string, 0, len(dataRows))

			for i, row := range dataRows {
				vals := make([]interface{}, len(validCols))
				for j, c := range validCols {
					if pos, ok := headerPos[c]; ok && pos < len(row) {
						cell := strings.TrimSpace(row[pos])
						if cell == "" {
							vals[j] = nil
						} else if c == "eff_from" || c == "eff_to" {
							if norm := NormalizeDate(cell); norm != "" {
								if t, err := time.Parse(constants.DateFormat, norm); err == nil {
									vals[j] = t
								} else {
									vals[j] = norm
								}
							}
						} else {
							vals[j] = cell
						}
					}
				}
				if pos, ok := headerPos["counterparty_code"]; ok && pos < len(row) {
					code := strings.TrimSpace(row[pos])
					if code != "" {
						counterpartyCodes = append(counterpartyCodes, code)
					}
				}
				copyRows[i] = vals
			}

			// === Step 5: Transaction (sync all inserts) ===
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()

			_, _ = tx.Exec(ctx, "SET LOCAL statement_timeout = '10min'")

			// Bulk insert counterparties
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"mastercounterparty"},
				validCols, pgx.CopyFromRows(copyRows)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError,
					"Failed to insert into mastercounterparty: "+err.Error())
				return
			}

			// Insert audit records immediately (sync)
			if len(counterpartyCodes) > 0 {
				auditSQL := `
					INSERT INTO auditactioncounterparty(counterparty_id, actiontype, processing_status, requested_by, requested_at)
					SELECT counterparty_id, 'CREATE', 'PENDING_APPROVAL', $1, now()
					FROM mastercounterparty
					WHERE counterparty_code = ANY($2);
				`
				if _, err := tx.Exec(ctx, auditSQL, userName, counterpartyCodes); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError,
						"Failed to insert audit records: "+err.Error())
					return
				}
			}

			// Final commit (sync)
			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
				return
			}
			committed = true

			batchIDs = append(batchIDs, uuid.New().String())
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			constants.ValueSuccess: true,
			"batch_ids":            batchIDs,
		})
	}
}

func UploadCounterpartyBankSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// --- user ---
		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			var req struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			userID = req.UserID
		}
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		userName := session.Name

		// --- counterparty_id (from form, required) ---
		counterpartyID := strings.TrimSpace(r.FormValue("counterparty_id"))
		if counterpartyID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_id required in form")
			return
		}

		// --- parse multipart ---
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseForm+err.Error())
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No file uploaded")
			return
		}

		allowed := map[string]bool{
			"bank_id":           true,
			"bank":              true,
			"country":           true,
			"branch":            true,
			"account":           true,
			"swift":             true,
			"rel":               true,
			"currency":          true,
			"category":          true,
			constants.KeyStatus: true,
		}

		batchIDs := []string{}

		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToOpenFile)
				return
			}
			records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty CSV file")
				return
			}

			// normalize headers
			headers := make([]string, len(records[0]))
			for i, h := range records[0] {
				hn := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(h, " ", "_")))
				hn = strings.Trim(hn, "\"',")
				headers[i] = hn
			}

			// build validCols from headers (only allowed columns)
			validCols := []string{}
			headerPos := map[string]int{}
			for i, h := range headers {
				headerPos[h] = i
				if allowed[h] {
					validCols = append(validCols, h)
				}
			}

			if len(validCols) == 0 {
				api.RespondWithError(w, http.StatusBadRequest, "No acceptable columns found in file")
				return
			}

			if !slices.Contains(validCols, "counterparty_id") {
				validCols = append(validCols, "counterparty_id")
			}

			rows := records[1:]
			copyRows := make([][]interface{}, len(rows))
			for i, row := range rows {
				vals := make([]interface{}, len(validCols))
				for j, col := range validCols {
					// if col is counterparty_id, set from form
					if col == "counterparty_id" {
						vals[j] = counterpartyID
						continue
					}
					// otherwise pull from CSV if present
					if pos, ok := headerPos[col]; ok && pos < len(row) {
						cell := strings.TrimSpace(row[pos])
						if cell == "" {
							vals[j] = nil
						} else {
							vals[j] = cell
						}
					} else {
						vals[j] = nil
					}
				}
				copyRows[i] = vals
			}

			// --- Sync transaction: COPY + single audit INSERT ---
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()

			// allow longer running COPY
			_, _ = tx.Exec(ctx, "SET LOCAL statement_timeout = '10min'")

			// COPY into mastercounterpartybanks
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"mastercounterpartybanks"}, validCols, pgx.CopyFromRows(copyRows)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "COPY failed: "+err.Error())
				return
			}

			// Validate banks, currencies, categories after staging
			if slices.Contains(validCols, "bank") {
				var invalidBanks []string
				bankQ := `SELECT DISTINCT UPPER(TRIM(bank)) FROM mastercounterpartybanks WHERE counterparty_id = $1 AND UPPER(TRIM(bank)) != '' AND UPPER(TRIM(bank)) != ALL($2)`
				bankRows, berr := tx.Query(ctx, bankQ, counterpartyID, api.GetBankNamesFromCtx(ctx))
				if berr == nil {
					for bankRows.Next() {
						var b string
						if bankRows.Scan(&b) == nil {
							invalidBanks = append(invalidBanks, b)
						}
					}
					bankRows.Close()
					if len(invalidBanks) > 0 {
						api.RespondWithError(w, http.StatusBadRequest, "Invalid or unauthorized banks: "+strings.Join(invalidBanks, ", "))
						return
					}
				}
			}

			if slices.Contains(validCols, "currency") {
				var invalidCurrs []string
				currQ := `SELECT DISTINCT UPPER(TRIM(currency)) FROM mastercounterpartybanks WHERE counterparty_id = $1 AND UPPER(TRIM(currency)) != '' AND UPPER(TRIM(currency)) != ALL($2)`
				currRows, cerr := tx.Query(ctx, currQ, counterpartyID, api.GetCurrencyCodesFromCtx(ctx))
				if cerr == nil {
					for currRows.Next() {
						var c string
						if currRows.Scan(&c) == nil {
							invalidCurrs = append(invalidCurrs, c)
						}
					}
					currRows.Close()
					if len(invalidCurrs) > 0 {
						api.RespondWithError(w, http.StatusBadRequest, "Invalid or unauthorized currencies: "+strings.Join(invalidCurrs, ", "))
						return
					}
				}
			}

			if slices.Contains(validCols, "category") {
				var invalidCategories []string
				catQ := `SELECT DISTINCT UPPER(TRIM(category)) FROM mastercounterpartybanks WHERE counterparty_id = $1 AND UPPER(TRIM(category)) != '' AND UPPER(TRIM(category)) != ALL($2)`
				catRows, caterr := tx.Query(ctx, catQ, counterpartyID, api.GetCashFlowCategoryNamesFromCtx(ctx))
				if caterr == nil {
					for catRows.Next() {
						var cat string
						if catRows.Scan(&cat) == nil {
							invalidCategories = append(invalidCategories, cat)
						}
					}
					catRows.Close()
					if len(invalidCategories) > 0 {
						api.RespondWithError(w, http.StatusBadRequest, "Invalid or unauthorized categories: "+strings.Join(invalidCategories, ", "))
						return
					}
				}
			}

			auditSQL := `
				INSERT INTO auditactioncounterparty(counterparty_id, actiontype, processing_status, requested_by, requested_at)
				VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, now())
			`
			if _, err := tx.Exec(ctx, auditSQL, counterpartyID, userName); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit record: "+err.Error())
				return
			}

			// commit
			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
				return
			}
			committed = true

			batchIDs = append(batchIDs, uuid.New().String())
		}

		// respond
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "batch_ids": batchIDs})
	}
}
