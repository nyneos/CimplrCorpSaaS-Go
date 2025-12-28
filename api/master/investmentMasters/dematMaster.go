package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlyDematError translates database errors into user-friendly messages
func getUserFriendlyDematError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	errMsg := err.Error()
	lower := strings.ToLower(errMsg)

	// Connection errors
	if strings.Contains(lower, "connection refused") || strings.Contains(lower, "no connection") {
		return "Database connection error. Please try again.", http.StatusServiceUnavailable
	}

	// Unique constraint: entity_name + demat_account_number
	if strings.Contains(lower, "unique_entity_demat_not_deleted") ||
		(strings.Contains(lower, "duplicate key") && strings.Contains(lower, "entity_name") && strings.Contains(lower, "demat_account_number")) {
		return "A demat account with this entity and account number already exists.", http.StatusOK
	}

	// Foreign key constraints
	if strings.Contains(lower, "foreign key") || strings.Contains(lower, "fk_") {
		if strings.Contains(lower, "auditactiondemat") {
			return "Cannot delete: demat account has pending approval actions.", http.StatusOK
		}
		if strings.Contains(lower, "dp_id") {
			return "Invalid DP ID. Please select a valid depository participant.", http.StatusOK
		}
		if strings.Contains(lower, "entity_name") {
			return "Invalid entity. Please select a valid entity.", http.StatusOK
		}
		if strings.Contains(lower, "default_settlement_account") {
			return "Invalid settlement account. Please select a valid bank account.", http.StatusOK
		}
		return "Referenced data not found. Please check your input.", http.StatusOK
	}

	// Check constraints
	if strings.Contains(lower, "check constraint") || strings.Contains(lower, "_ck") {
		if strings.Contains(lower, "status") || strings.Contains(lower, "masterdemat_status_ck") {
			return "Invalid status. Must be one of: Active, Inactive, Closed.", http.StatusOK
		}
		if strings.Contains(lower, "source") || strings.Contains(lower, "masterdemat_source_ck") {
			return "Invalid source. Must be one of: Manual, Upload, ERP.", http.StatusOK
		}
		if strings.Contains(lower, "depository") || strings.Contains(lower, "masterdemataccount_depository_check") {
			return "Invalid depository. Must be either NSDL or CDSL.", http.StatusOK
		}
		if strings.Contains(lower, "actiontype") || strings.Contains(lower, "auditactiondemat_actiontype_ck") {
			return "Invalid action type. Must be one of: CREATE, EDIT, DELETE.", http.StatusOK
		}
		if strings.Contains(lower, "processing_status") || strings.Contains(lower, "auditactiondemat_processing_status_ck") {
			return "Invalid processing status.", http.StatusOK
		}
		return "Data validation failed. Please check your input values.", http.StatusOK
	}

	// Not null constraints
	if strings.Contains(lower, "not null") || strings.Contains(lower, "null value") {
		if strings.Contains(lower, "entity_name") {
			return "Entity name is required.", http.StatusOK
		}
		if strings.Contains(lower, "dp_id") {
			return "DP ID is required.", http.StatusOK
		}
		if strings.Contains(lower, "depository") {
			return "Depository is required.", http.StatusOK
		}
		if strings.Contains(lower, "demat_account_number") {
			return "Demat account number is required.", http.StatusOK
		}
		if strings.Contains(lower, "default_settlement_account") {
			return "Default settlement account is required.", http.StatusOK
		}
		if strings.Contains(lower, "actiontype") {
			return "Action type is required.", http.StatusOK
		}
		if strings.Contains(lower, "processing_status") {
			return "Processing status is required.", http.StatusOK
		}
		return "Required field is missing.", http.StatusOK
	}

	// Timeout errors
	if strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline exceeded") {
		return "Operation timed out. Please try again.", http.StatusServiceUnavailable
	}

	// Default fallback
	return fmt.Sprintf("Operation failed: %s", context), http.StatusInternalServerError
}

type CreateDematRequestSingle struct {
	UserID                   string `json:"user_id"`
	EntityName               string `json:"entity_name"`
	DPID                     string `json:"dp_id"`
	Depository               string `json:"depository"`
	DematAccountNumber       string `json:"demat_account_number"`
	DepositoryParticipant    string `json:"depository_participant,omitempty"`
	ClientID                 string `json:"client_id,omitempty"`
	DefaultSettlementAccount string `json:"default_settlement_account"`
	Status                   string `json:"status,omitempty"`
	Source                   string `json:"source,omitempty"` // ignored; set to Manual
}

type CreateDematBulkRequest struct {
	UserID string `json:"user_id"`
	Rows   []struct {
		EntityName               string `json:"entity_name"`
		DPID                     string `json:"dp_id"`
		Depository               string `json:"depository"`
		DematAccountNumber       string `json:"demat_account_number"`
		DepositoryParticipant    string `json:"depository_participant,omitempty"`
		ClientID                 string `json:"client_id,omitempty"`
		DefaultSettlementAccount string `json:"default_settlement_account"`
		Status                   string `json:"status,omitempty"`
	} `json:"rows"`
}

type UpdateDematRequest struct {
	UserID  string                 `json:"user_id"`
	DematID string                 `json:"demat_id"`
	Fields  map[string]interface{} `json:"fields"`
	Reason  string                 `json:"reason"`
}

type UploadDematResult struct {
	Success bool   `json:"success"`
	BatchID string `json:"batch_id,omitempty"`
	Error   string `json:"error,omitempty"`
}

func UploadDematSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			var tmp struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&tmp)
			userID = tmp.UserID
		}
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		userName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "user not in active sessions")
			return
		}

		// Get context-based access controls
		approvedEntities, _ := ctx.Value(api.BusinessUnitsKey).([]string)
		if len(approvedEntities) == 0 {
			api.RespondWithError(w, http.StatusForbidden, "No accessible entities")
			return
		}

		approvedDPs, _ := ctx.Value("ApprovedDPs").([]map[string]string)
		approvedBankAccounts, _ := ctx.Value("ApprovedBankAccounts").([]map[string]string)

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
			"entity_name":                true,
			"dp_id":                      true,
			"depository":                 true,
			"demat_account_number":       true,
			"depository_participant":     true,
			"client_id":                  true,
			"default_settlement_account": true,
			constants.KeyStatus:          true,
			// source auto-populated
		}

		results := make([]UploadDematResult, 0, len(files))

		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToOpenFile)
				return
			}
			records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidOrEmptyFile)
				return
			}

			headers := normalizeHeader(records[0])
			dataRows := records[1:]

			validCols := []string{}
			headerPos := map[string]int{}
			for i, h := range headers {
				headerPos[h] = i
				if allowed[h] {
					validCols = append(validCols, h)
				}
			}

			// require mandatory columns
			mandatories := []string{"entity_name", "dp_id", "depository", "demat_account_number", "default_settlement_account"}
			for _, m := range mandatories {
				if !contains(validCols, m) {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("CSV must include column: %s", m))
					return
				}
			}

			// Validate all rows before processing
			for i, row := range dataRows {
				var entityName, dpID, settlementAcc string
				if pos, ok := headerPos["entity_name"]; ok && pos < len(row) {
					entityName = strings.TrimSpace(row[pos])
				}
				if pos, ok := headerPos["dp_id"]; ok && pos < len(row) {
					dpID = strings.TrimSpace(row[pos])
				}
				var dpParticipant string
				if pos, ok := headerPos["depository_participant"]; ok && pos < len(row) {
					dpParticipant = strings.TrimSpace(row[pos])
				}
				if pos, ok := headerPos["default_settlement_account"]; ok && pos < len(row) {
					settlementAcc = strings.TrimSpace(row[pos])
				}

				// Validate entity
				entityFound := false
				for _, e := range approvedEntities {
					if strings.EqualFold(e, entityName) {
						entityFound = true
						break
					}
				}
				if !entityFound {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Row %d: Entity '%s' not in accessible entities", i+1, entityName))
					return
				}

				// Validate DP (match against dp_id/dp_name/dp_code for either dp_id or depository_participant)
				dpFound := false
				for _, dp := range approvedDPs {
					if dp["dp_id"] == dpID || strings.EqualFold(dp["dp_name"], dpID) || dp["dp_code"] == dpID ||
						(dpParticipant != "" && (dp["dp_id"] == dpParticipant || strings.EqualFold(dp["dp_name"], dpParticipant) || dp["dp_code"] == dpParticipant)) {
						dpFound = true
						break
					}
				}
				if !dpFound {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Row %d: DP '%s' not in approved DPs", i+1, dpID))
					return
				}

				// Validate bank account
				accountFound := false
				for _, acc := range approvedBankAccounts {
					if acc["account_number"] == settlementAcc || acc["account_id"] == settlementAcc {
						accountFound = true
						break
					}
				}
				if !accountFound {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Row %d: Settlement account '%s' not in approved bank accounts", i+1, settlementAcc))
					return
				}
			}

			// prepare COPY rows
			copyRows := make([][]interface{}, len(dataRows))
			dematNumbers := make([]string, 0, len(dataRows))
			for i, row := range dataRows {
				vals := make([]interface{}, len(validCols))
				for j, c := range validCols {
					if pos, ok := headerPos[c]; ok && pos < len(row) {
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
				if pos, ok := headerPos["demat_account_number"]; ok && pos < len(row) {
					if code := strings.TrimSpace(row[pos]); code != "" {
						dematNumbers = append(dematNumbers, code)
					}
				}
				copyRows[i] = vals
			}

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

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"investment", "masterdemataccount"}, validCols, pgx.CopyFromRows(copyRows)); err != nil {
				msg, status := getUserFriendlyDematError(err, "uploading demat accounts")
				api.RespondWithError(w, status, msg)
				return
			}

			// set source = 'Upload' for inserted rows
			if len(dematNumbers) > 0 {
				if _, err := tx.Exec(ctx, `
					UPDATE investment.masterdemataccount
					SET source = 'Upload'
					WHERE demat_account_number = ANY($1)
				`, dematNumbers); err != nil {
					msg, status := getUserFriendlyDematError(err, "updating source")
					api.RespondWithError(w, status, msg)
					return
				}

				// insert audit rows for created demats (map demat_account_number -> demat_id)
				if _, err := tx.Exec(ctx, `
					INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, reason, requested_by, requested_at)
					SELECT demat_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now()
					FROM investment.masterdemataccount
					WHERE demat_account_number = ANY($2)
				`, userName, dematNumbers); err != nil {
					msg, status := getUserFriendlyDematError(err, "creating audit record")
					api.RespondWithError(w, status, msg)
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
				return
			}
			committed = true

			results = append(results, UploadDematResult{Success: true, BatchID: uuid.New().String()})
		}

		api.RespondWithPayload(w, true, "", results)
	}
}

func CreateDematSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateDematRequestSingle
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		// required fields
		if strings.TrimSpace(req.EntityName) == "" ||
			strings.TrimSpace(req.DPID) == "" ||
			strings.TrimSpace(req.Depository) == "" ||
			strings.TrimSpace(req.DematAccountNumber) == "" ||
			strings.TrimSpace(req.DefaultSettlementAccount) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing required demat fields")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Validate entity access
		approvedEntities, _ := ctx.Value(api.BusinessUnitsKey).([]string)
		entityFound := false
		for _, e := range approvedEntities {
			if strings.EqualFold(e, req.EntityName) {
				entityFound = true
				break
			}
		}
		if !entityFound {
			api.RespondWithError(w, http.StatusForbidden, "Access denied. Entity not in accessible entities")
			return
		}

		// Validate DP
		approvedDPs, _ := ctx.Value("ApprovedDPs").([]map[string]string)
		dpFound := false
		for _, dp := range approvedDPs {
			if dp["dp_id"] == req.DPID || strings.EqualFold(dp["dp_name"], req.DPID) || dp["dp_code"] == req.DPID ||
				(req.DepositoryParticipant != "" && (dp["dp_id"] == req.DepositoryParticipant || strings.EqualFold(dp["dp_name"], req.DepositoryParticipant) || dp["dp_code"] == req.DepositoryParticipant)) {
				dpFound = true
				break
			}
		}
		if !dpFound {
			api.RespondWithError(w, http.StatusBadRequest, "DP not in approved DPs")
			return
		}

		// Validate bank account
		approvedBankAccounts, _ := ctx.Value("ApprovedBankAccounts").([]map[string]string)
		accountFound := false
		for _, acc := range approvedBankAccounts {
			if acc["account_number"] == req.DefaultSettlementAccount || acc["account_id"] == req.DefaultSettlementAccount {
				accountFound = true
				break
			}
		}
		if !accountFound {
			api.RespondWithError(w, http.StatusBadRequest, "Settlement account not in approved bank accounts")
			return
		}

		// uniqueness: entity_name + demat_account_number
		var tmp int
		err := pgxPool.QueryRow(ctx, `SELECT 1 FROM investment.masterdemataccount WHERE entity_name=$1 AND demat_account_number=$2 AND COALESCE(is_deleted,false)=false LIMIT 1`, req.EntityName, req.DematAccountNumber).Scan(&tmp)
		if err == nil {
			api.RespondWithError(w, http.StatusBadRequest, "Demat account (entity_name + demat_account_number) already exists")
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		insertQ := `
			INSERT INTO investment.masterdemataccount (
				entity_name, dp_id, depository, demat_account_number,
				depository_participant, client_id, default_settlement_account, status, source
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'Manual')
			RETURNING demat_id
		`
		var dematID string
		if err := tx.QueryRow(ctx, insertQ,
			req.EntityName,
			req.DPID,
			req.Depository,
			req.DematAccountNumber,
			req.DepositoryParticipant,
			req.ClientID,
			req.DefaultSettlementAccount,
			defaultIfEmpty(req.Status, "Active"),
		).Scan(&dematID); err != nil {
			msg, status := getUserFriendlyDematError(err, "creating demat account")
			api.RespondWithError(w, status, msg)
			return
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
		`, dematID, userEmail); err != nil {
			msg, status := getUserFriendlyDematError(err, "creating audit record")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"demat_id":  dematID,
			"entity":    req.EntityName,
			"source":    "Manual",
			"requested": userEmail,
		})
	}
}

// ---------------------------
// CreateDematBulk (JSON array) - similar to DP CreateBulk
// ---------------------------
func CreateDematBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateDematBulkRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoRowsProvided)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		approvedEntities, _ := ctx.Value(api.BusinessUnitsKey).([]string)
		approvedDPs, _ := ctx.Value("ApprovedDPs").([]map[string]string)
		approvedBankAccounts, _ := ctx.Value("ApprovedBankAccounts").([]map[string]string)

		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			name := strings.TrimSpace(row.EntityName)
			dpid := strings.TrimSpace(row.DPID)
			dep := strings.TrimSpace(row.Depository)
			dacc := strings.TrimSpace(row.DematAccountNumber)
			settle := strings.TrimSpace(row.DefaultSettlementAccount)
			if name == "" || dpid == "" || dep == "" || dacc == "" || settle == "" {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: constants.ErrMissingRequiredFieldsUser,
				})
				continue
			}

			// Validate entity
			entityFound := false
			for _, e := range approvedEntities {
				if strings.EqualFold(e, name) {
					entityFound = true
					break
				}
			}
			if !entityFound {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "entity": name, constants.ValueError: "Entity not in accessible entities"})
				continue
			}

			// Validate DP (allow matching via dpid or depository_participant)
			dpFound := false
			for _, dp := range approvedDPs {
				if dp["dp_id"] == dpid || strings.EqualFold(dp["dp_name"], dpid) || dp["dp_code"] == dpid ||
					(row.DepositoryParticipant != "" && (dp["dp_id"] == row.DepositoryParticipant || strings.EqualFold(dp["dp_name"], row.DepositoryParticipant) || dp["dp_code"] == row.DepositoryParticipant)) {
					dpFound = true
					break
				}
			}
			if !dpFound {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "entity": name, constants.ValueError: "DP not in approved DPs"})
				continue
			}

			// Validate bank account
			accountFound := false
			for _, acc := range approvedBankAccounts {
				if acc["account_number"] == settle || acc["account_id"] == settle {
					accountFound = true
					break
				}
			}
			if !accountFound {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "entity": name, constants.ValueError: "Settlement account not in approved bank accounts"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxBeginFailed + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			// uniqueness check
			var exists int
			err = tx.QueryRow(ctx, `
				SELECT 1 FROM investment.masterdemataccount
				WHERE entity_name=$1 AND demat_account_number=$2 AND COALESCE(is_deleted,false)=false LIMIT 1
			`, name, dacc).Scan(&exists)
			if err == nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "entity": name, constants.ValueError: "Duplicate demat (entity+demat_account_number)"})
				continue
			}

			var dematID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.masterdemataccount (
					entity_name, dp_id, depository, demat_account_number,
					depository_participant, client_id, default_settlement_account, status, source
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'Manual')
				RETURNING demat_id
			`, name, dpid, dep, dacc, row.DepositoryParticipant, row.ClientID, settle, defaultIfEmpty(row.Status, "Active")).Scan(&dematID); err != nil {
				msg, _ := getUserFriendlyDematError(err, "creating demat account")
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "entity": name, constants.ValueError: msg})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, requested_by, requested_at)
				VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
			`, dematID, userEmail); err != nil {
				msg, _ := getUserFriendlyDematError(err, "creating audit record")
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "demat_id": dematID, constants.ValueError: msg})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "demat_id": dematID, constants.ValueError: constants.ErrCommitFailedCapitalized + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{constants.ValueSuccess: true, "demat_id": dematID, "entity_name": name})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

func UpdateDemat(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateDematRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.DematID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "demat_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFieldsToUpdateUser)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT entity_name, dp_id, depository, demat_account_number,
			       depository_participant, client_id, default_settlement_account, status, source
			FROM investment.masterdemataccount
			WHERE demat_id=$1
			FOR UPDATE
		`
		var oldVals [9]interface{}
		if err := tx.QueryRow(ctx, sel, req.DematID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8],
		); err != nil {
			msg, status := getUserFriendlyDematError(err, "fetching demat account")
			api.RespondWithError(w, status, msg)
			return
		}

		fieldPairs := map[string]int{
			"entity_name":                0,
			"dp_id":                      1,
			"depository":                 2,
			"demat_account_number":       3,
			"depository_participant":     4,
			"client_id":                  5,
			"default_settlement_account": 6,
			constants.KeyStatus:          7,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
				// if demat_account_number changed, ensure uniqueness
				if lk == "demat_account_number" {
					newVal := strings.TrimSpace(fmt.Sprint(v))
					if newVal != "" && newVal != ifaceToString(oldVals[3]) {
						var exists int
						err := tx.QueryRow(ctx, `SELECT 1 FROM investment.masterdemataccount WHERE demat_account_number=$1 AND COALESCE(is_deleted,false)=false AND demat_id <> $2 LIMIT 1`, newVal, req.DematID).Scan(&exists)
						if err == nil {
							api.RespondWithError(w, http.StatusBadRequest, "demat_account_number already exists")
							return
						}
					}
				}

				oldField := "old_" + lk
				sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, lk, pos, oldField, pos+1))
				args = append(args, v, oldVals[idx])
				pos += 2
			}
		}

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no valid updatable fields found")
			return
		}

		q := fmt.Sprintf("UPDATE investment.masterdemataccount SET %s WHERE demat_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.DematID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			msg, status := getUserFriendlyDematError(err, "updating demat account")
			api.RespondWithError(w, status, msg)
			return
		}

		// insert audit record
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
		`, req.DematID, req.Reason, userEmail); err != nil {
			msg, status := getUserFriendlyDematError(err, "creating audit record")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"demat_id":  req.DematID,
			"requested": userEmail,
		})
	}
}
func UpdateDematBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				DematID string                 `json:"demat_id"`
				Fields  map[string]interface{} `json:"fields"`
				Reason  string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if row.DematID == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "demat_id missing"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "demat_id": row.DematID, constants.ValueError: constants.ErrTxBeginFailedCapitalized + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			// fetch existing
			sel := `
				SELECT entity_name, dp_id, depository, demat_account_number,
				       depository_participant, client_id, default_settlement_account, status, source
				FROM investment.masterdemataccount WHERE demat_id=$1 FOR UPDATE`
			var oldVals [9]interface{}
			if err := tx.QueryRow(ctx, sel, row.DematID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
				&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8],
			); err != nil {
				msg, _ := getUserFriendlyDematError(err, "fetching demat account")
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "demat_id": row.DematID, constants.ValueError: msg})
				continue
			}

			fieldPairs := map[string]int{
				"entity_name":                0,
				"dp_id":                      1,
				"depository":                 2,
				"demat_account_number":       3,
				"depository_participant":     4,
				"client_id":                  5,
				"default_settlement_account": 6,
				constants.KeyStatus:          7,
			}

			var sets []string
			var args []interface{}
			pos := 1

			for k, v := range row.Fields {
				lk := strings.ToLower(k)
				if idx, ok := fieldPairs[lk]; ok {
					if lk == "demat_account_number" {
						newVal := strings.TrimSpace(fmt.Sprint(v))
						if newVal != "" && newVal != ifaceToString(oldVals[3]) {
							var exists int
							err := tx.QueryRow(ctx, `SELECT 1 FROM investment.masterdemataccount WHERE demat_account_number=$1 AND COALESCE(is_deleted,false)=false AND demat_id <> $2 LIMIT 1`, newVal, row.DematID).Scan(&exists)
							if err == nil {
								results = append(results, map[string]interface{}{constants.ValueSuccess: false, "demat_id": row.DematID, constants.ValueError: "demat_account_number already exists: " + newVal})
								continue
							}
						}
					}
					oldField := "old_" + lk
					sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, lk, pos, oldField, pos+1))
					args = append(args, v, oldVals[idx])
					pos += 2
				}
			}

			if len(sets) == 0 {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "demat_id": row.DematID, constants.ValueError: "No valid fields"})
				continue
			}

			q := fmt.Sprintf("UPDATE investment.masterdemataccount SET %s WHERE demat_id=$%d", strings.Join(sets, ", "), pos)
			args = append(args, row.DematID)

			if _, err := tx.Exec(ctx, q, args...); err != nil {
				msg, _ := getUserFriendlyDematError(err, "updating demat account")
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "demat_id": row.DematID, constants.ValueError: msg})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
			`, row.DematID, row.Reason, userEmail); err != nil {
				msg, _ := getUserFriendlyDematError(err, "creating audit record")
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "demat_id": row.DematID, constants.ValueError: msg})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "demat_id": row.DematID, constants.ValueError: constants.ErrCommitFailed + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{constants.ValueSuccess: true, "demat_id": row.DematID, "requested": userEmail})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

func DeleteDemat(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			DematIDs []string `json:"demat_ids"`
			Reason   string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if len(req.DematIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "demat_ids required")
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Email
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.DematIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())
			`, id, req.Reason, requestedBy); err != nil {
				msg, status := getUserFriendlyDematError(err, "creating delete request")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.DematIDs})
	}
}

func BulkApproveDematActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			DematIDs []string `json:"demat_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (demat_id) action_id, demat_id, actiontype, processing_status
			FROM investment.auditactiondemat
			WHERE demat_id = ANY($1)
			ORDER BY demat_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.DematIDs)
		if err != nil {
			msg, status := getUserFriendlyDematError(err, "fetching audit actions")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		var toApprove []string
		var toDeleteActionIDs []string
		var deleteMasterIDs []string

		for rows.Next() {
			var aid, did, atype, pstatus string
			if err := rows.Scan(&aid, &did, &atype, &pstatus); err != nil {
				continue
			}
			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			if ps == "APPROVED" {
				continue
			}
			if ps == "PENDING_DELETE_APPROVAL" {
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteMasterIDs = append(deleteMasterIDs, did)
				continue
			}
			if ps == "PENDING_APPROVAL" || ps == "PENDING_EDIT_APPROVAL" {
				toApprove = append(toApprove, aid)
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrNoApprovableActions, map[string]any{
				"approved_action_ids": []string{},
				"deleted_demats":      []string{},
			})
			return
		}

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactiondemat
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove); err != nil {
				msg, status := getUserFriendlyDematError(err, "approving actions")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactiondemat
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				msg, status := getUserFriendlyDematError(err, "marking actions as deleted")
				api.RespondWithError(w, status, msg)
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE investment.masterdemataccount
				SET is_deleted=true, status='Inactive'
				WHERE demat_id = ANY($1)
			`, deleteMasterIDs); err != nil {
				msg, status := getUserFriendlyDematError(err, "soft-deleting demat accounts")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"approved_action_ids": toApprove,
			"deleted_demats":      deleteMasterIDs,
		})
	}
}

func BulkRejectDematActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			DematIDs []string `json:"demat_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (demat_id) action_id, demat_id, processing_status
			FROM investment.auditactiondemat
			WHERE demat_id = ANY($1)
			ORDER BY demat_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.DematIDs)
		if err != nil {
			msg, status := getUserFriendlyDematError(err, "fetching audit actions")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		actionIDs := []string{}
		cannotReject := []string{}
		found := map[string]bool{}
		for rows.Next() {
			var aid, sid, ps string
			if err := rows.Scan(&aid, &sid, &ps); err != nil {
				continue
			}
			found[sid] = true
			if strings.ToUpper(strings.TrimSpace(ps)) == "APPROVED" {
				cannotReject = append(cannotReject, sid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		missing := []string{}
		for _, id := range req.DematIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for demat_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved demat_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactiondemat
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE action_id = ANY($3)
		`, checkerBy, req.Comment, actionIDs); err != nil {
			msg, status := getUserFriendlyDematError(err, "rejecting actions")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

func GetDematsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Get context-based filtering
		approvedEntities, _ := ctx.Value(api.BusinessUnitsKey).([]string)
		if len(approvedEntities) == 0 {
			api.RespondWithError(w, http.StatusForbidden, "No accessible entities")
			return
		}

		approvedDPs, _ := ctx.Value("ApprovedDPs").([]map[string]string)
		dpIDs := make([]string, 0, len(approvedDPs))
		for _, dp := range approvedDPs {
			if dp["dp_id"] != "" {
				dpIDs = append(dpIDs, dp["dp_id"])
			}
		}

		approvedBankAccounts, _ := ctx.Value("ApprovedBankAccounts").([]map[string]string)
		accountNumbers := make([]string, 0, len(approvedBankAccounts))
		for _, acc := range approvedBankAccounts {
			if acc["account_number"] != "" {
				accountNumbers = append(accountNumbers, acc["account_number"])
			}
		}

		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.demat_id)
					a.demat_id,
					a.actiontype,
					a.processing_status,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactiondemat a
				ORDER BY a.demat_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					demat_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactiondemat
				GROUP BY demat_id
			),
			clearing AS (
				SELECT 
					c.account_id,
					STRING_AGG(c.code_type || ':' || c.code_value, ', ') AS clearing_codes
				FROM public.masterclearingcode c
				GROUP BY c.account_id
			)
			SELECT
				m.*,
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,
				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				COALESCE(h.deleted_by,'') AS deleted_by,
				COALESCE(h.deleted_at,'') AS deleted_at,
				ba.account_id,
				ba.account_number,
				ba.account_nickname,
				b.bank_name,
				COALESCE(e.entity_name, ec.entity_name) AS account_entity_name,
				cl.clearing_codes
			FROM investment.masterdemataccount m
			LEFT JOIN latest_audit l ON l.demat_id = m.demat_id
			LEFT JOIN history h ON h.demat_id = m.demat_id
			LEFT JOIN public.masterbankaccount ba ON ba.account_number = m.default_settlement_account
			LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
			LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
			LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
			LEFT JOIN clearing cl ON cl.account_id = ba.account_id
			WHERE COALESCE(m.is_deleted,false)=false
			  AND m.entity_name = ANY($1)
			  AND (m.dp_id = ANY($2) OR m.depository_participant = ANY($2))
			  AND m.default_settlement_account = ANY($3)
			ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC;
		`

		rows, err := pgxPool.Query(ctx, q, approvedEntities, dpIDs, accountNumbers)
		if err != nil {
			msg, status := getUserFriendlyDematError(err, "fetching demat accounts")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)

		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))

			for i, f := range fields {
				name := string(f.Name)
				if vals[i] == nil {
					rec[name] = ""
				} else if t, ok := vals[i].(time.Time); ok {
					rec[name] = t.Format(constants.DateTimeFormat)
				} else {
					rec[name] = vals[i]
				}
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func GetApprovedActiveDemats(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Get context-based filtering
		approvedEntities, _ := ctx.Value(api.BusinessUnitsKey).([]string)
		if len(approvedEntities) == 0 {
			api.RespondWithError(w, http.StatusForbidden, "No accessible entities")
			return
		}

		approvedDPs, _ := ctx.Value("ApprovedDPs").([]map[string]string)
		dpIDs := make([]string, 0, len(approvedDPs))
		for _, dp := range approvedDPs {
			if dp["dp_id"] != "" {
				dpIDs = append(dpIDs, dp["dp_id"])
			}
		}

		approvedBankAccounts, _ := ctx.Value("ApprovedBankAccounts").([]map[string]string)
		accountNumbers := make([]string, 0, len(approvedBankAccounts))
		for _, acc := range approvedBankAccounts {
			if acc["account_number"] != "" {
				accountNumbers = append(accountNumbers, acc["account_number"])
			}
		}

		q := `
			WITH latest AS (
				SELECT DISTINCT ON (demat_id)
					demat_id,
					processing_status
				FROM investment.auditactiondemat
				ORDER BY demat_id, requested_at DESC
			)
			SELECT
				m.demat_id,
				m.dp_id,
				m.depository,
				m.demat_account_number,
				m.depository_participant,
				m.client_id,
				m.default_settlement_account
			FROM investment.masterdemataccount m
			JOIN latest l ON l.demat_id = m.demat_id
			WHERE 
				UPPER(l.processing_status) = 'APPROVED'
				AND UPPER(m.status) = 'ACTIVE'
				AND COALESCE(m.is_deleted, false) = false
				AND m.entity_name = ANY($1)
				AND (m.dp_id = ANY($2) OR m.depository_participant = ANY($2))
				AND m.default_settlement_account = ANY($3)
			ORDER BY m.demat_account_number;
		`

		rows, err := pgxPool.Query(ctx, q, approvedEntities, dpIDs, accountNumbers)
		if err != nil {
			msg, status := getUserFriendlyDematError(err, "fetching active demat accounts")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		var out []map[string]interface{}
		for rows.Next() {
			var dematID, dpID, depository, dematAccountNumber, defaultAccount string
			var depositoryParticipant, clientID *string

			if err := rows.Scan(&dematID, &dpID, &depository, &dematAccountNumber, &depositoryParticipant, &clientID, &defaultAccount); err != nil {
				msg, status := getUserFriendlyDematError(err, "scanning row")
				api.RespondWithError(w, status, msg)
				return
			}

			out = append(out, map[string]interface{}{
				"demat_id":                   dematID,
				"dp_id":                      dpID,
				"depository":                 depository,
				"demat_account_number":       dematAccountNumber,
				"depository_participant":     ifNotNil(depositoryParticipant),
				"client_id":                  ifNotNil(clientID),
				"default_settlement_account": defaultAccount,
			})
		}

		if rows.Err() != nil {
			msg, status := getUserFriendlyDematError(rows.Err(), "iterating rows")
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func ifNotNil(s *string) string {
	if s != nil {
		return *s
	}
	return ""
}
