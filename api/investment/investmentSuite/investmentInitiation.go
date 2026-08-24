package investmentsuite

import (
	"CimplrCorpSaas/api"
	approvalengine "CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/api/policyengine/common"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"
	"CimplrCorpSaas/internal/validation"
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

// submitMFInitiationForApproval fires the approval-matrix engine for a
// newly created or edited MF investment initiation, asynchronously.
// submittedByUserID must be the session's numeric user_id (hard FK to
// public.users(id) — never a display name or email).
func submitMFInitiationForApproval(pool *pgxpool.Pool, initiationID, entityName, submittedByUserID, actorEmail, txType, matrixID string, amount float64) {
	go func() {
		bgCtx := context.Background()
		if _, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
			ModuleCode:       "INVESTMENT_MF",
			EntityCode:       entityName,
			TransactionType:  txType,
			RecordID:         initiationID,
			RecordTable:      "investment.investment_initiation",
			AuditTable:       "investment.auditactioninitiation",
			AuditIDColumn:    "initiation_id",
			ActionType:       strings.TrimPrefix(txType, "MF_INITIATION_"),
			Amount:           amount,
			SubmittedBy:      submittedByUserID,
			SubmittedByEmail: actorEmail,
			MatrixID:         matrixID,
		}); err != nil {
			api.LogError("[MFInitiation] approvalengine.CreateInstance failed for initiation %s (%s): %v", initiationID, txType, err)
		}
	}()
}

// loadMFInitiationApprovalWorkflow finds the most recent INVESTMENT_MF
// approval-matrix instance for this initiation and returns its rich detail
// for the ApprovalWorkflowViewer UI. Self-heals if no instance exists yet but
// a PENDING% audit row does. auditactioninitiation stores the requester's
// real email in requested_by, so resolving a real user_id only needs a join
// on public.users.email.
func loadMFInitiationApprovalWorkflow(ctx context.Context, pool *pgxpool.Pool, initiationID, viewerUserID string) interface{} {
	if initiationID == "" {
		return nil
	}

	var instanceID string
	_ = pool.QueryRow(ctx, `
		SELECT instance_id
		FROM uam.approval_instance
		WHERE record_id = $1 AND module_code = 'INVESTMENT_MF' AND is_deleted = false
		ORDER BY submitted_at DESC LIMIT 1`, initiationID,
	).Scan(&instanceID)

	if instanceID == "" {
		var pendingActionType, entityName, submittedByUserID, submittedByEmail string
		var amount float64
		scanErr := pool.QueryRow(ctx, `
			SELECT a.actiontype, COALESCE(i.entity_name,''), COALESCE(u.id,''), COALESCE(a.requested_by,''),
			       COALESCE(i.amount,0)
			FROM investment.auditactioninitiation a
			JOIN investment.investment_initiation i ON i.initiation_id = a.initiation_id
			LEFT JOIN public.users u ON u.email = a.requested_by
			WHERE a.initiation_id = $1
			  AND a.processing_status LIKE 'PENDING%'
			ORDER BY a.requested_at DESC LIMIT 1`, initiationID,
		).Scan(&pendingActionType, &entityName, &submittedByUserID, &submittedByEmail, &amount)

		if scanErr == nil && pendingActionType != "" && submittedByUserID != "" {
			txType := map[string]string{
				"CREATE": "MF_INITIATION_CREATE",
				"EDIT":   "MF_INITIATION_EDIT",
				"DELETE": "MF_INITIATION_DELETE",
			}[pendingActionType]
			if txType == "" {
				txType = "MF_INITIATION_CREATE"
			}
			newInstID, instErr := approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
				ModuleCode:       "INVESTMENT_MF",
				EntityCode:       entityName,
				TransactionType:  txType,
				RecordID:         initiationID,
				RecordTable:      "investment.investment_initiation",
				AuditTable:       "investment.auditactioninitiation",
				AuditIDColumn:    "initiation_id",
				ActionType:       pendingActionType,
				Amount:           amount,
				SubmittedBy:      submittedByUserID,
				SubmittedByEmail: submittedByEmail,
				MatrixID:         "",
			})
			if instErr != nil {
				api.LogError("[MFInitiation] Self-heal CreateInstance for %s: %v", initiationID, instErr)
			} else if newInstID != "" {
				instanceID = newInstID
				api.LogInfo("[MFInitiation] Self-heal: created instance %s for initiation %s", newInstID, initiationID)
			}
		} else if scanErr == nil && pendingActionType != "" {
			api.LogInfo("[MFInitiation] Self-heal skipped for %s: requester email did not resolve to a unique user_id", initiationID)
		}
	}

	if instanceID == "" {
		return nil
	}
	richDetail, richErr := approvalengine.GetRichInstanceDetail(ctx, pool, instanceID, viewerUserID)
	if richErr != nil {
		api.LogError("[MFInitiation] GetRichInstanceDetail failed for instance=%s initiation=%s: %v", instanceID, initiationID, richErr)
		return nil
	}
	return richDetail
}

// errInitiationLoadFailedSuffix is appended to a record id when a row load
// fails while processing a bulk action, to build the per-item error message.
const errInitiationLoadFailedSuffix = ": load failed: "

// ---------------------------
// Request/Response Types
// ---------------------------

type CreateInitiationRequestSingle struct {
	UserID          string  `json:"user_id"`
	ProposalID      string  `json:"proposal_id,omitempty"`
	TransactionDate string  `json:"transaction_date"` // YYYY-MM-DD
	EntityName      string  `json:"entity_name"`
	SchemeID        string  `json:"scheme_id"`
	FolioID         string  `json:"folio_id,omitempty"`
	DematID         string  `json:"demat_id,omitempty"`
	Amount          float64 `json:"amount"`
	Source          string  `json:"source,omitempty"`
}

type UpdateInitiationRequest struct {
	UserID       string                 `json:"user_id"`
	InitiationID string                 `json:"initiation_id"`
	Fields       map[string]interface{} `json:"fields"`
	Reason       string                 `json:"reason"`
}

type UploadInitiationResult struct {
	Success bool   `json:"success"`
	BatchID string `json:"batch_id,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ---------------------------
// UploadInitiationSimple (bulk CSV/XLSX -> COPY -> audit)
// ---------------------------

// ---------------------------
// CreateInitiationSingle (single create, source='Manual')
// ---------------------------

func CreateInitiationSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateInitiationRequestSingle
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		// validate required fields
		if strings.TrimSpace(req.TransactionDate) == "" ||
			strings.TrimSpace(req.EntityName) == "" ||
			strings.TrimSpace(req.SchemeID) == "" ||
			req.Amount <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Missing required fields: transaction_date, entity_name, scheme_id, amount")
			return
		}

		// Either folio_id OR demat_id must be provided
		if strings.TrimSpace(req.FolioID) == "" && strings.TrimSpace(req.DematID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Either folio_id or demat_id is required")
			return
		}

		// if source is Proposal, proposal_id is required
		source := defaultIfEmpty(req.Source, "Manual")
		if strings.ToUpper(source) == "PROPOSAL" && strings.TrimSpace(req.ProposalID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "proposal_id is required when source is 'Proposal'")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		if errMsg := validation.ValidateMFMasterReferences(r.Context(), map[string]interface{}{
			"entity_name": req.EntityName,
			"scheme_id":   req.SchemeID,
			"folio_id":    req.FolioID,
			"demat_id":    req.DematID,
		}); errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}

		ctx := r.Context()

		// Block re-initiation if an active (non-rejected) initiation already exists for this proposal+scheme
		if strings.TrimSpace(req.ProposalID) != "" && strings.TrimSpace(req.SchemeID) != "" {
			var activeCount int
			checkErr := pgxPool.QueryRow(ctx, `
				SELECT COUNT(*) FROM investment.investment_initiation ii
				JOIN (
					SELECT DISTINCT ON (initiation_id)
						initiation_id, processing_status
					FROM investment.auditactioninitiation
					ORDER BY initiation_id, requested_at DESC
				) latest ON latest.initiation_id = ii.initiation_id
				WHERE ii.proposal_id = $1
				  AND ii.scheme_id = $2
				  AND UPPER(latest.processing_status) NOT IN ('REJECTED','CANCELLED','DELETED')
			`, req.ProposalID, req.SchemeID).Scan(&activeCount)
			if checkErr == nil && activeCount > 0 {
				api.RespondWithError(w, http.StatusBadRequest,
					"An active initiation already exists for this proposal and scheme. Delete the existing initiation before creating a new one.")
				return
			}
		}

		createRow := mfInitiationRow{
			ProposalID:      req.ProposalID,
			TransactionDate: req.TransactionDate,
			EntityName:      req.EntityName,
			SchemeID:        req.SchemeID,
			FolioID:         req.FolioID,
			DematID:         req.DematID,
			Amount:          req.Amount,
			Source:          source,
		}
		ok, createMatrixID := mfEnforceMatrix(ctx, w, r, pgxPool, enforceCtx{
			EventCode:   common.TriggerPreCreate,
			HandlerName: "CreateInitiationSingle",
			APIPath:     "/investment/initiation/create",
			SubModule:   mfSubInitiation,
			EntityCode:  req.EntityName,
			Actor:       userEmail,
		}, buildMFInitiationPolicyFields(createRow))
		if !ok {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		insertQ := `
				INSERT INTO investment.investment_initiation (
					proposal_id, transaction_date, entity_name, scheme_id, folio_id, demat_id, amount, source
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				RETURNING initiation_id
			`
		var initiationID string
		proposalID := nullIfEmpty(req.ProposalID)
		folioID := nullIfEmpty(req.FolioID)
		dematID := nullIfEmpty(req.DematID)
		if err := tx.QueryRow(ctx, insertQ,
			proposalID,
			req.TransactionDate,
			req.EntityName,
			req.SchemeID,
			folioID,
			dematID,
			req.Amount,
			source,
		).Scan(&initiationID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactioninitiation (initiation_id, actiontype, processing_status, requested_by, requested_at, requested_ip)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now(), $3)
		`, initiationID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		// if this initiation is tied to a proposal+scheme, mark the allocation's initiation_status
		if strings.TrimSpace(req.ProposalID) != "" && strings.TrimSpace(req.SchemeID) != "" {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.investment_proposal_allocation
				SET initiation_status = true, updated_at = now()
				WHERE proposal_id = $1 AND scheme_id = $2
			`, req.ProposalID, req.SchemeID); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to update allocation initiation_status: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		// Fire approval-matrix engine instance (async, no-op if engine disabled or no matrix)
		submitMFInitiationForApproval(pgxPool, initiationID, req.EntityName, req.UserID, userEmail, "MF_INITIATION_CREATE", createMatrixID, req.Amount)

		go func(iID, uID, uEmail string) {
			pl := BuildInitiationNotifPayload(context.Background(), pgxPool, []string{iID}, constants.AuditActionCreate, uEmail)
			catalog.TriggerNotification(
				context.Background(), pgxPool,
				"/investment/initiation/create",
				fmt.Sprintf("INVESTMENT_INITIATION_CREATE/%s/%d", uID, time.Now().UnixMilli()),
				pl.ToMap(),
			)
			dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_MF", "MF_INITIATION", "POST_CREATE", []string{iID}, uEmail)
		}(initiationID, req.UserID, userEmail)

		response := map[string]any{
			"initiation_id": initiationID,
			"entity_name":   req.EntityName,
			"source":        source,
			"requested":     userEmail,
		}
		if req.ProposalID != "" {
			response["proposal_id"] = req.ProposalID
		}
		api.RespondWithPayload(w, true, "", response)
	}
}

// ---------------------------
// CreateInitiationBulk (multiple JSON rows)
// ---------------------------

func CreateInitiationBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				ProposalID      string  `json:"proposal_id,omitempty"`
				TransactionDate string  `json:"transaction_date"`
				EntityName      string  `json:"entity_name"`
				SchemeID        string  `json:"scheme_id"`
				FolioID         string  `json:"folio_id,omitempty"`
				DematID         string  `json:"demat_id,omitempty"`
				Amount          float64 `json:"amount"`
				Source          string  `json:"source,omitempty"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoRowsProvided)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			proposalID := strings.TrimSpace(row.ProposalID)
			txnDate := strings.TrimSpace(row.TransactionDate)
			entityName := strings.TrimSpace(row.EntityName)
			schemeID := strings.TrimSpace(row.SchemeID)
			source := defaultIfEmpty(row.Source, "Manual")

			if txnDate == "" || entityName == "" || schemeID == "" || row.Amount <= 0 {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: "Missing required fields: transaction_date, entity_name, scheme_id, amount",
				})
				continue
			}

			// Either folio_id OR demat_id must be provided
			if strings.TrimSpace(row.FolioID) == "" && strings.TrimSpace(row.DematID) == "" {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: "Either folio_id or demat_id is required",
				})
				continue
			}

			// if source is Proposal, proposal_id is required
			if strings.ToUpper(source) == "PROPOSAL" && proposalID == "" {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: "proposal_id is required when source is 'Proposal'",
				})
				continue
			}

			// Entity scope + MF master validation per row
			if errMsg := validation.ValidateMFMasterReferences(ctx, map[string]interface{}{
				"entity_name": entityName,
				"scheme_id":   schemeID,
				"folio_id":    row.FolioID,
				"demat_id":    row.DematID,
			}); errMsg != "" {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: errMsg,
				})
				continue
			}

			bulkCreateRow := mfInitiationRow{
				ProposalID:      proposalID,
				TransactionDate: txnDate,
				EntityName:      entityName,
				SchemeID:        schemeID,
				FolioID:         row.FolioID,
				DematID:         row.DematID,
				Amount:          row.Amount,
				Source:          source,
			}
			ok, pmsg, _ := mfEnforceInlineWithMatrix(ctx, r, pgxPool, enforceCtx{
				EventCode:   common.TriggerPreCreate,
				HandlerName: "CreateInitiationBulk",
				APIPath:     "/investment/initiation/create-bulk",
				SubModule:   mfSubInitiation,
				EntityCode:  entityName,
				Actor:       userEmail,
			}, buildMFInitiationPolicyFields(bulkCreateRow))
			if !ok {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: pmsg,
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxBeginFailed + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			var initiationID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.investment_initiation (
					proposal_id, transaction_date, entity_name, scheme_id, folio_id, demat_id, amount, source
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				RETURNING initiation_id
			`, nullIfEmpty(proposalID), txnDate, entityName, schemeID, nullIfEmpty(row.FolioID), nullIfEmpty(row.DematID), row.Amount, source).Scan(&initiationID); err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "proposal_id": proposalID, constants.ValueError: "Insert failed: " + err.Error(),
				})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactioninitiation (initiation_id, actiontype, processing_status, requested_by, requested_at, requested_ip)
				VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now(), $3)
			`, initiationID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "initiation_id": initiationID, constants.ValueError: constants.ErrAuditInsertFailed + err.Error(),
				})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "initiation_id": initiationID, constants.ValueError: constants.ErrCommitFailedCapitalized + err.Error(),
				})
				continue
			}

			dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_MF", "MF_INITIATION", "POST_CREATE", []string{initiationID}, userEmail)

			result := map[string]interface{}{
				constants.ValueSuccess: true,
				"initiation_id":        initiationID,
				"entity_name":          entityName,
				"source":               source,
			}
			if proposalID != "" {
				result["proposal_id"] = proposalID
			}

			// mark allocation initiation_status true for this row if linked to a proposal+scheme
			if proposalID != "" && schemeID != "" {
				if _, err := pgxPool.Exec(ctx, `
					UPDATE investment.investment_proposal_allocation
					SET initiation_status = true, updated_at = now()
					WHERE proposal_id = $1 AND scheme_id = $2
				`, proposalID, schemeID); err != nil {
					// attach error to result but continue processing other rows
					result["warning"] = "failed to flag allocation initiation_status: " + err.Error()
				}
			}
			results = append(results, result)
		}

		// Fire bulk-create notification for all successfully created IDs
		createdIDs := make([]string, 0)
		for _, r := range results {
			if ok, _ := r[constants.ValueSuccess].(bool); ok {
				if id, _ := r["initiation_id"].(string); id != "" {
					createdIDs = append(createdIDs, id)
				}
			}
		}
		if len(createdIDs) > 0 {
			ids := createdIDs
			uID := req.UserID
			uEmail := userEmail
			go func() {
				pl := BuildInitiationNotifPayload(context.Background(), pgxPool, ids, constants.AuditActionCreate, uEmail)
				catalog.TriggerNotification(
					context.Background(), pgxPool,
					"/investment/initiation/bulk-create",
					fmt.Sprintf("INVESTMENT_INITIATION_BULK_CREATE/%s/%d", uID, time.Now().UnixMilli()),
					pl.ToMap(),
				)
			}()
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// UpdateInitiation (single update)
// ---------------------------

func UpdateInitiation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateInitiationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.InitiationID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "initiation_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFieldsToUpdateUser)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		existingRow, loadErr := loadMFInitiationRow(ctx, pgxPool, req.InitiationID)
		if loadErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "load failed: "+loadErr.Error())
			return
		}
		updatedRow := applyMFInitiationEdits(existingRow, req.Fields)
		if ok, _ := mfEnforceMatrix(ctx, w, r, pgxPool, enforceCtx{
			EventCode:   common.TriggerPreEdit,
			HandlerName: "UpdateInitiation",
			APIPath:     "/investment/initiation/update",
			SubModule:   mfSubInitiation,
			EntityCode:  req.InitiationID,
			Actor:       userEmail,
		}, buildMFInitiationPolicyFields(updatedRow)); !ok {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// fetch existing values
		sel := `
			SELECT proposal_id, transaction_date, entity_name, scheme_id, folio_id, demat_id, amount, source
			FROM investment.investment_initiation
			WHERE initiation_id=$1
			FOR UPDATE
		`
		var oldVals [8]interface{}
		if err := tx.QueryRow(ctx, sel, req.InitiationID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"proposal_id":      0,
			"transaction_date": 1,
			"entity_name":      2,
			"scheme_id":        3,
			"folio_id":         4,
			"demat_id":         5,
			"amount":           6,
			"source":           7,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
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

		q := fmt.Sprintf("UPDATE investment.investment_initiation SET %s, updated_at=now() WHERE initiation_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.InitiationID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
			return
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactioninitiation (initiation_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
			VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now(), $4)
		`, req.InitiationID, req.Reason, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		go func(iID, uID, uEmail string) {
			pl := BuildInitiationNotifPayload(context.Background(), pgxPool, []string{iID}, "UPDATE", uEmail)
			catalog.TriggerNotification(
				context.Background(), pgxPool,
				"/investment/initiation/update",
				fmt.Sprintf("INVESTMENT_INITIATION_UPDATE/%s/%d", uID, time.Now().UnixMilli()),
				pl.ToMap(),
			)
		}(req.InitiationID, req.UserID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]any{
			"initiation_id": req.InitiationID,
			"requested":     userEmail,
		})
	}
}

// ---------------------------
// UpdateInitiationBulk
// ---------------------------

func UpdateInitiationBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				InitiationID string                 `json:"initiation_id"`
				Fields       map[string]interface{} `json:"fields"`
				Reason       string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if row.InitiationID == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "initiation_id missing"})
				continue
			}

			bulkExistingRow, loadErr := loadMFInitiationRow(ctx, pgxPool, row.InitiationID)
			if loadErr != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "initiation_id": row.InitiationID, constants.ValueError: "load failed: " + loadErr.Error()})
				continue
			}
			bulkUpdatedRow := applyMFInitiationEdits(bulkExistingRow, row.Fields)
			ok, pmsg, _ := mfEnforceInlineWithMatrix(ctx, r, pgxPool, enforceCtx{
				EventCode:   common.TriggerPreEdit,
				HandlerName: "UpdateInitiationBulk",
				APIPath:     "/investment/initiation/update-bulk",
				SubModule:   mfSubInitiation,
				EntityCode:  row.InitiationID,
				Actor:       userEmail,
			}, buildMFInitiationPolicyFields(bulkUpdatedRow))
			if !ok {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "initiation_id": row.InitiationID, constants.ValueError: pmsg,
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "initiation_id": row.InitiationID, constants.ValueError: constants.ErrTxBeginFailedCapitalized + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			sel := `
				SELECT proposal_id, transaction_date, entity_name, scheme_id, folio_id, amount, source
				FROM investment.investment_initiation WHERE initiation_id=$1 FOR UPDATE`
			var oldVals [7]interface{}
			if err := tx.QueryRow(ctx, sel, row.InitiationID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
				&oldVals[4], &oldVals[5], &oldVals[6],
			); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "initiation_id": row.InitiationID, constants.ValueError: "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"proposal_id":      0,
				"transaction_date": 1,
				"entity_name":      2,
				"scheme_id":        3,
				"folio_id":         4,
				"amount":           5,
				"source":           6,
			}

			var sets []string
			var args []interface{}
			pos := 1

			for k, v := range row.Fields {
				lk := strings.ToLower(k)
				if idx, ok := fieldPairs[lk]; ok {
					oldField := "old_" + lk
					sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, lk, pos, oldField, pos+1))
					args = append(args, v, oldVals[idx])
					pos += 2
				}
			}

			if len(sets) == 0 {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "initiation_id": row.InitiationID, constants.ValueError: "No valid fields"})
				continue
			}

			q := fmt.Sprintf("UPDATE investment.investment_initiation SET %s, updated_at=now() WHERE initiation_id=$%d", strings.Join(sets, ", "), pos)
			args = append(args, row.InitiationID)

			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "initiation_id": row.InitiationID, constants.ValueError: constants.ErrUpdateFailed + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactioninitiation (initiation_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
				VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now(), $4)
			`, row.InitiationID, row.Reason, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "initiation_id": row.InitiationID, constants.ValueError: constants.ErrAuditInsertFailed + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "initiation_id": row.InitiationID, constants.ValueError: constants.ErrCommitFailed + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{constants.ValueSuccess: true, "initiation_id": row.InitiationID, "requested": userEmail})
		}

		// Fire bulk-update notification for all successful IDs
		updatedIDs := make([]string, 0)
		for _, r := range results {
			if ok, _ := r[constants.ValueSuccess].(bool); ok {
				if id, _ := r["initiation_id"].(string); id != "" {
					updatedIDs = append(updatedIDs, id)
				}
			}
		}
		if len(updatedIDs) > 0 {
			ids := updatedIDs
			uID := req.UserID
			uEmail := userEmail
			go func() {
				pl := BuildInitiationNotifPayload(context.Background(), pgxPool, ids, "UPDATE", uEmail)
				catalog.TriggerNotification(
					context.Background(), pgxPool,
					"/investment/initiation/bulk-update",
					fmt.Sprintf("INVESTMENT_INITIATION_BULK_UPDATE/%s/%d", uID, time.Now().UnixMilli()),
					pl.ToMap(),
				)
			}()
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// DeleteInitiation
// ---------------------------

func DeleteInitiation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			InitiationIDs []string `json:"initiation_ids"`
			Reason        string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if len(req.InitiationIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "initiation_ids required")
			return
		}

		requestedBy := api.GetUserEmailFromCtx(r.Context())
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		deleteMatrixByID := map[string]string{}
		for _, id := range req.InitiationIDs {
			delRow, loadErr := loadMFInitiationRow(ctx, pgxPool, id)
			if loadErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, id+errInitiationLoadFailedSuffix+loadErr.Error())
				return
			}
			ok, pmsg, tID := mfEnforceInlineWithMatrix(ctx, r, pgxPool, enforceCtx{
				EventCode:   common.TriggerPreDelete,
				HandlerName: "DeleteInitiation",
				APIPath:     "/investment/initiation/delete",
				SubModule:   mfSubInitiation,
				EntityCode:  id,
				Actor:       requestedBy,
			}, buildMFInitiationPolicyFields(delRow))
			if !ok {
				api.RespondWithError(w, http.StatusUnprocessableEntity, id+": "+pmsg)
				return
			}
			deleteMatrixByID[id] = tID
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.InitiationIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactioninitiation (initiation_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
				VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now(), $4)
			`, id, req.Reason, api.SystemIfBlank(requestedBy), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "insert failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		// ── Approval-matrix engine: cancel stale instances and create DELETE instances.
		for _, id := range req.InitiationIDs {
			if err := approvalengine.CancelPendingInstances(context.Background(), pgxPool, "INVESTMENT_MF", id, requestedBy); err != nil {
				api.LogError("[MFInitiation] CancelPendingInstances for delete failed for %s: %v", id, err)
			}
			initiationRow, _ := loadMFInitiationRow(context.Background(), pgxPool, id)
			submitMFInitiationForApproval(pgxPool, id, initiationRow.EntityName, req.UserID, requestedBy, "MF_INITIATION_DELETE", deleteMatrixByID[id], initiationRow.Amount)
		}

		go func(ids []string, uID, uEmail string) {
			pl := BuildInitiationNotifPayload(context.Background(), pgxPool, ids, "DELETE_REQUEST", uEmail)
			catalog.TriggerNotification(
				context.Background(), pgxPool,
				"/investment/initiation/delete-request",
				fmt.Sprintf("INVESTMENT_INITIATION_DELETE_REQUEST/%s/%d", uID, time.Now().UnixMilli()),
				pl.ToMap(),
			)
		}(append([]string{}, req.InitiationIDs...), req.UserID, requestedBy)

		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.InitiationIDs})
	}
}

// ---------------------------
// BulkApproveInitiationActions
// ---------------------------

func BulkApproveInitiationActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			InitiationIDs []string `json:"initiation_ids"`
			Comment       string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		checkerBy := api.GetUserEmailFromCtx(r.Context())
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		for _, id := range req.InitiationIDs {
			apprRow, loadErr := loadMFInitiationRow(ctx, pgxPool, id)
			if loadErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, id+errInitiationLoadFailedSuffix+loadErr.Error())
				return
			}
			if ok, pmsg := mfEnforceInline(ctx, r, pgxPool, enforceCtx{
				EventCode:   common.TriggerPreApprove,
				HandlerName: "BulkApproveInitiationActions",
				APIPath:     "/investment/initiation/approve",
				SubModule:   mfSubInitiation,
				EntityCode:  id,
				Actor:       checkerBy,
			}, buildMFInitiationPolicyFields(apprRow)); !ok {
				api.RespondWithError(w, http.StatusUnprocessableEntity, id+": "+pmsg)
				return
			}
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (initiation_id) action_id, initiation_id, actiontype, processing_status
			FROM investment.auditactioninitiation
			WHERE initiation_id = ANY($1)
			  AND UPPER(COALESCE(actiontype, '')) <> 'UPLOAD_FILE'
			ORDER BY initiation_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.InitiationIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var toApprove []string            // action_ids to approve
		var toApproveInitiations []string // initiation_ids corresponding to actions being approved
		var editInitiationIDs []string
		var toDeleteActionIDs []string
		var deleteMasterIDs []string

		for rows.Next() {
			var aid, iid, atype, pstatus string
			if err := rows.Scan(&aid, &iid, &atype, &pstatus); err != nil {
				continue
			}
			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			if ps == constants.StatusApproved {
				continue
			}
			if ps == constants.StatusPendingDeleteApproval {
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteMasterIDs = append(deleteMasterIDs, iid)
				continue
			}
			if ps == constants.StatusPendingApproval || ps == constants.StatusPendingEditApproval {
				toApprove = append(toApprove, aid)
				toApproveInitiations = append(toApproveInitiations, iid)
				if ps == constants.StatusPendingEditApproval {
					editInitiationIDs = append(editInitiationIDs, iid)
				}
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrNoApprovableActions, map[string]any{
				"approved_initiation_ids": []string{},
				"deleted_initiations":     []string{},
			})
			return
		}

		// ── Approval-matrix engine: handle engine-managed records first.
		// Records the engine handled (Acted==true) are skipped in legacy stamp below.
		engineActed := map[string]bool{}
		for _, iid := range req.InitiationIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{
				ModuleCode: "INVESTMENT_MF", RecordID: iid,
				UserID: req.UserID, UserEmail: checkerBy,
				Action: approvalengine.ActionApproved, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[MFInitiation] ActOnPendingOrDiagnose approve failed for %s: %v", iid, actionErr)
				continue
			}
			if actionRes.Acted {
				engineActed[iid] = true
			} else if actionRes.CancelledStale {
				api.LogInfo("[MFInitiation] cancelled stale instance for %s", iid)
			} else if actionRes.Reason != "" {
				api.LogInfo("[MFInitiation] engine skipped %s: %s", iid, actionRes.Reason)
			}
		}
		// Remove engine-handled IDs from legacy stamp lists
		filterEA := func(ids []string) []string {
			out := ids[:0]
			for _, id := range ids {
				if !engineActed[id] {
					out = append(out, id)
				}
			}
			return out
		}
		toApprove = filterEA(toApprove)
		toApproveInitiations = filterEA(toApproveInitiations)
		toDeleteActionIDs = filterEA(toDeleteActionIDs)
		deleteMasterIDs = filterEA(deleteMasterIDs)

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactioninitiation
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				WHERE action_id = ANY($4)
			`, api.SystemIfBlank(checkerBy), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), toApprove); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "approve update failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactioninitiation
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				WHERE action_id = ANY($4)
			`, api.SystemIfBlank(checkerBy), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), toDeleteActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "mark deleted failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE investment.investment_initiation
				SET is_deleted=true, status='Inactive', updated_at=now()
				WHERE initiation_id = ANY($1)
			`, deleteMasterIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "master soft-delete failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		// ensure slices are non-nil so JSON marshals empty arrays instead of null
		if toApproveInitiations == nil {
			toApproveInitiations = []string{}
		}
		if deleteMasterIDs == nil {
			deleteMasterIDs = []string{}
		}

		if len(toApproveInitiations) > 0 {
			ids := append([]string{}, toApproveInitiations...)
			editSet := make(map[string]struct{}, len(editInitiationIDs))
			for _, id := range editInitiationIDs {
				editSet[id] = struct{}{}
			}
			uID := req.UserID
			uEmail := checkerBy
			go func() {
				pl := BuildInitiationNotifPayload(context.Background(), pgxPool, ids, constants.AuditActionApprove, uEmail)
				catalog.TriggerNotification(
					context.Background(), pgxPool,
					"/investment/initiation/approve",
					fmt.Sprintf("INVESTMENT_INITIATION_APPROVE/%s/%d", uID, time.Now().UnixMilli()),
					pl.ToMap(),
				)
				for _, id := range ids {
					trigger := "POST_APPROVE"
					if _, isEdit := editSet[id]; isEdit {
						trigger = "POST_EDIT"
					}
					dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_MF", "MF_INITIATION", trigger, []string{id}, uEmail)
				}
			}()
		}
		if len(deleteMasterIDs) > 0 {
			ids := append([]string{}, deleteMasterIDs...)
			uID := req.UserID
			uEmail := checkerBy
			go func() {
				pl := BuildInitiationNotifPayload(context.Background(), pgxPool, ids, constants.AuditActionDelete, uEmail)
				catalog.TriggerNotification(
					context.Background(), pgxPool,
					"/investment/initiation/delete",
					fmt.Sprintf("INVESTMENT_INITIATION_DELETE/%s/%d", uID, time.Now().UnixMilli()),
					pl.ToMap(),
				)
				for _, id := range ids {
					dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_MF", "MF_INITIATION", "POST_DELETE", []string{id}, uEmail)
				}
			}()
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"approved_initiation_ids": toApproveInitiations,
			"deleted_initiations":     deleteMasterIDs,
		})
	}
}

// ---------------------------
// BulkRejectInitiationActions
// ---------------------------

func BulkRejectInitiationActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			InitiationIDs []string `json:"initiation_ids"`
			Comment       string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		checkerBy := api.GetUserEmailFromCtx(r.Context())
		if checkerBy == "" {
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
			SELECT DISTINCT ON (initiation_id) action_id, initiation_id, processing_status
			FROM investment.auditactioninitiation
			WHERE initiation_id = ANY($1)
			  AND UPPER(COALESCE(actiontype, '')) <> 'UPLOAD_FILE'
			ORDER BY initiation_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.InitiationIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := []string{}
		cannotReject := []string{}
		found := map[string]bool{}
		for rows.Next() {
			var aid, iid, ps string
			if err := rows.Scan(&aid, &iid, &ps); err != nil {
				continue
			}
			found[iid] = true
			if strings.ToUpper(strings.TrimSpace(ps)) == constants.StatusApproved {
				cannotReject = append(cannotReject, iid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		missing := []string{}
		for _, id := range req.InitiationIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for initiation_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved initiation_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		for _, id := range req.InitiationIDs {
			rejRow, loadErr := loadMFInitiationRow(ctx, pgxPool, id)
			if loadErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, id+errInitiationLoadFailedSuffix+loadErr.Error())
				return
			}
			if ok, pmsg := mfEnforceInline(ctx, r, pgxPool, enforceCtx{
				EventCode:   common.TriggerPreReject,
				HandlerName: "BulkRejectInitiationActions",
				APIPath:     "/investment/initiation/reject",
				SubModule:   mfSubInitiation,
				EntityCode:  id,
				Actor:       checkerBy,
			}, buildMFInitiationPolicyFields(rejRow)); !ok {
				api.RespondWithError(w, http.StatusUnprocessableEntity, id+": "+pmsg)
				return
			}
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactioninitiation
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
			WHERE action_id = ANY($4)
		`, api.SystemIfBlank(checkerBy), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), actionIDs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
			return
		}

		// ── Approval-matrix engine: reject engine-managed records
		for _, iid := range req.InitiationIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{
				ModuleCode: "INVESTMENT_MF", RecordID: iid,
				UserID: req.UserID, UserEmail: checkerBy,
				Action: approvalengine.ActionRejected, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[MFInitiation] ActOnPendingOrDiagnose reject failed for %s: %v", iid, actionErr)
			}
			if actionRes.Acted {
				api.LogInfo("[MFInitiation] engine rejected %s", iid)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		go func(ids []string, uID, uEmail string) {
			pl := BuildInitiationNotifPayload(context.Background(), pgxPool, ids, constants.AuditActionReject, uEmail)
			catalog.TriggerNotification(
				context.Background(), pgxPool,
				"/investment/initiation/reject",
				fmt.Sprintf("INVESTMENT_INITIATION_REJECT/%s/%d", uID, time.Now().UnixMilli()),
				pl.ToMap(),
			)
			for _, id := range ids {
				dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_MF", "MF_INITIATION", "POST_REJECT", []string{id}, uEmail)
			}
		}(append([]string{}, req.InitiationIDs...), req.UserID, checkerBy)

		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

// ---------------------------
// GetApprovedActiveInitiations
// ---------------------------

func GetApprovedActiveInitiations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.initiation_id)
					a.initiation_id, a.actiontype, a.processing_status, a.action_id,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason
				FROM investment.auditactioninitiation a
				WHERE UPPER(COALESCE(a.actiontype, '')) <> 'UPLOAD_FILE'
				ORDER BY a.initiation_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					initiation_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactioninitiation
				GROUP BY initiation_id
			)
			SELECT
				m.*, 
				COALESCE(s.scheme_id::text, m.scheme_id::text) AS scheme_id,
				COALESCE(s.scheme_name, m.scheme_id) AS scheme_name,
				COALESCE(s.amc_name,'') AS amc_name,
				COALESCE(f.folio_number,'') AS folio_number,
				COALESCE(f.folio_id::text,'') AS folio_id,
				COALESCE(d.default_settlement_account,'') AS demat_number,
				COALESCE(d.demat_id::text,'') AS demat_id,
				DATE_PART('day',  m.transaction_date::timestamp-now()::timestamp)::int AS age_days,
				COALESCE(m.amount,0) AS gross_investment_amount,
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS requested_by,
				TO_CHAR((l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR((l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,
				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				COALESCE(h.deleted_by,'') AS deleted_by,
				COALESCE(h.deleted_at,'') AS deleted_at,
				COALESCE(nav.nav_value,0) AS nav,
				TO_CHAR(nav.nav_date,'YYYY-MM-DD') AS applicable_nav_date
			FROM investment.investment_initiation m
			LEFT JOIN latest_audit l ON l.initiation_id = m.initiation_id
			LEFT JOIN history h ON h.initiation_id = m.initiation_id
			-- allow flexible matching: the initiation column may contain either the id or the human-friendly value
			-- LEFT JOIN investment.masterscheme s ON (s.scheme_id::text = m.scheme_id OR s.scheme_name = m.scheme_id)
			LEFT JOIN investment.masterscheme s ON (
       COALESCE(s.is_deleted, false) = false
       AND (
         (NULLIF(TRIM(m.scheme_id), '') IS NOT NULL AND s.scheme_id::text = TRIM(m.scheme_id))
         OR (NULLIF(TRIM(m.scheme_id), '') IS NOT NULL AND s.internal_scheme_code = TRIM(m.scheme_id))
         OR (NULLIF(TRIM(m.scheme_id), '') IS NOT NULL AND s.amfi_scheme_code = TRIM(m.scheme_id))
       )
)

			LEFT JOIN investment.masterfolio f ON (f.folio_id::text = m.folio_id OR f.folio_number = m.folio_id)
			LEFT JOIN investment.masterdemataccount d ON (
				d.demat_id::text = m.demat_id OR
				d.default_settlement_account = m.demat_id OR
				d.demat_account_number = m.demat_id
			)
			LEFT JOIN LATERAL (
				SELECT ans.nav_value, ans.nav_date
				FROM investment.amfi_nav_staging ans
				WHERE ans.nav_date IS NOT NULL
				  AND (
				    (COALESCE(s.amfi_scheme_code,'') <> '' AND ans.scheme_code::text = s.amfi_scheme_code)
				    OR (COALESCE(s.isin,'') <> '' AND (ans.isin_div_payout_growth = s.isin OR ans.isin_div_reinvestment = s.isin))
				  )
				ORDER BY ans.nav_date DESC
				LIMIT 1
			) nav ON true
			WHERE UPPER(COALESCE(l.processing_status,'')) = 'APPROVED'
					AND COALESCE(m.is_deleted, false) = false
					AND m.initiation_id NOT IN (
						SELECT initiation_id FROM investment.investment_confirmation
						WHERE COALESCE(is_deleted, false) = false
					)
		`
		args := []interface{}{}
		pos := 1
		if entityNames := suiteEntityNameRefs(ctx); len(entityNames) > 0 {
			q += fmt.Sprintf(" AND (COALESCE(m.entity_name,'') = '' OR m.entity_name = ANY($%d::text[]))", pos)
			args = append(args, entityNames)
			pos++
		}
		if schemeRefs := suiteMFSchemeRefs(ctx); len(schemeRefs) > 0 {
			q += fmt.Sprintf(" AND m.scheme_id = ANY($%d::text[])", pos)
			args = append(args, schemeRefs)
			pos++
		}
		if folioRefs := suiteMFFolioRefs(ctx); len(folioRefs) > 0 {
			q += fmt.Sprintf(" AND (COALESCE(m.folio_id,'') = '' OR m.folio_id = ANY($%d::text[]))", pos)
			args = append(args, folioRefs)
			pos++
		}
		if dematRefs := suiteMFDematRefs(ctx); len(dematRefs) > 0 {
			q += fmt.Sprintf(" AND (COALESCE(m.demat_id,'') = '' OR m.demat_id = ANY($%d::text[]))", pos)
			args = append(args, dematRefs)
		}
		q += " ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC"
		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					rec[string(f.Name)] = ""
				} else {
					if t, ok := vals[i].(time.Time); ok {
						rec[string(f.Name)] = t.Format(constants.DateTimeFormat)
					} else {
						rec[string(f.Name)] = vals[i]
					}
				}
			}

			// Fetch NAV from MFapi if not available in local database
			navValue, _ := rec["nav"].(float64)
			if navValue == 0 {
				// Try to get AMFI scheme code to fetch from MFapi
				var amfiSchemeCode string

				schemeID, _ := rec["scheme_id"].(string)
				if schemeID != "" {
					err := pgxPool.QueryRow(ctx, `
						SELECT COALESCE(amfi_scheme_code, '')
						FROM investment.masterscheme
						WHERE scheme_id = $1
						LIMIT 1
					`, schemeID).Scan(&amfiSchemeCode)

					// Only try the numeric AMFI scheme code — MFapi doesn't accept internal codes
					codesToTry := []string{}
					if err == nil && amfiSchemeCode != "" {
						codesToTry = append(codesToTry, amfiSchemeCode)
					}

					// Try each code until we get a valid NAV
					navFetched := false
					for _, code := range codesToTry {
						apiURL := fmt.Sprintf("https://api.mfapi.in/mf/%s", code)
						resp, err := http.Get(apiURL)
						if err == nil && resp.StatusCode == 200 {
							body, _ := io.ReadAll(resp.Body)
							resp.Body.Close()

							var apiResp struct {
								Meta struct {
									SchemeCode string `json:"scheme_code"`
								} `json:"meta"`
								Data []struct {
									Date string `json:"date"`
									NAV  string `json:"nav"`
								} `json:"data"`
								Status string `json:"status"`
							}

							if json.Unmarshal(body, &apiResp) == nil && len(apiResp.Data) > 0 {
								// Get the latest NAV (first entry)
								latestEntry := apiResp.Data[0]
								if navVal, err := strconv.ParseFloat(latestEntry.NAV, 64); err == nil && navVal > 0 {
									rec["nav"] = navVal
									rec["applicable_nav_date"] = latestEntry.Date
									rec["nav_source"] = "MFapi"
									navFetched = true
									break // Found valid NAV, stop trying
								}
							}
						} else if resp != nil {
							resp.Body.Close()
						}
					}

					// If still no NAV after trying all codes, mark as unavailable
					if !navFetched {
						rec["nav_source"] = "Unavailable"
					}
				} else {
					rec["nav_source"] = "Unavailable"
				}
			} else {
				rec["nav_source"] = "Local"
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

// ---------------------------
// GetInitiationsWithAudit
// ---------------------------

func GetInitiationsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		out, err := fetchInitiationRows(ctx, pgxPool, nil)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// fetchInitiationRows is the single source-of-truth query for initiations + audit + history + scheme/folio/demat joins.
// Pass ids=nil (or empty) to return ALL non-deleted initiations (used by GetInitiationsWithAudit).
// Pass a non-empty ids slice to filter by initiation_id = ANY(ids) (used by notif payload builder).
func fetchInitiationRows(ctx context.Context, pgxPool *pgxpool.Pool, ids []string) ([]map[string]interface{}, error) {
	const baseSQL = `
		WITH latest_audit AS (
			SELECT DISTINCT ON (a.initiation_id)
				a.initiation_id, a.actiontype, a.processing_status, a.action_id,
				a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason
			FROM investment.auditactioninitiation a
			WHERE UPPER(COALESCE(a.actiontype, '')) <> 'UPLOAD_FILE'
			ORDER BY a.initiation_id, a.requested_at DESC
		),
		history AS (
			SELECT 
				initiation_id,
				MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
				MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
				MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
				MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
				MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
				MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
			FROM investment.auditactioninitiation
			GROUP BY initiation_id
		)
		SELECT
			m.*, 
			COALESCE(s.scheme_id::text, m.scheme_id::text) AS scheme_id,
			COALESCE(s.scheme_name, m.scheme_id) AS scheme_name,
			COALESCE(s.amc_name,'') AS amc_name,
			COALESCE(f.folio_number,'') AS folio_number,
			COALESCE(f.folio_id::text,'') AS folio_id,
			COALESCE(d.default_settlement_account,'') AS demat_number,
			COALESCE(d.demat_id::text,'') AS demat_id,
			DATE_PART('day', now()::timestamp - m.transaction_date::timestamp)::int AS age_days,
			COALESCE(m.amount,0) AS gross_investment_amount,
			COALESCE(l.actiontype,'') AS action_type,
			COALESCE(l.processing_status,'') AS processing_status,
			COALESCE(l.action_id::text,'') AS action_id,
			COALESCE(l.requested_by,'') AS requested_by,
			TO_CHAR((l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') AS requested_at,
			COALESCE(l.checker_by,'') AS checker_by,
			TO_CHAR((l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') AS checker_at,
			COALESCE(l.checker_comment,'') AS checker_comment,
			COALESCE(l.reason,'') AS reason,
			COALESCE(h.created_by,'') AS created_by,
			COALESCE(h.created_at,'') AS created_at,
			COALESCE(h.edited_by,'') AS edited_by,
			COALESCE(h.edited_at,'') AS edited_at,
			COALESCE(h.deleted_by,'') AS deleted_by,
			COALESCE(h.deleted_at,'') AS deleted_at,
			COALESCE(nav.nav_value,0) AS nav,
			TO_CHAR(nav.nav_date,'YYYY-MM-DD') AS applicable_nav_date,
			-- ── Approval-engine columns ────────────────────────────────────────
			COALESCE(ai.instance_id,'')        AS approval_instance_id,
			COALESCE(ai.status,'')             AS approval_engine_status,
			COALESCE(aie.instance_eye_id,'')   AS current_eye_id,
			COALESCE(aie.position::text,'')    AS current_eye_position,
			COALESCE(aie.approvals_required,0) AS approvals_required,
			COALESCE(aie.approvals_received,0) AS approvals_received,
			aie.sla_deadline                   AS sla_deadline,
			COALESCE(aie.is_escalated,false)   AS is_escalated
		FROM investment.investment_initiation m
		LEFT JOIN latest_audit l ON l.initiation_id = m.initiation_id
		LEFT JOIN history h ON h.initiation_id = m.initiation_id
		LEFT JOIN investment.masterscheme s ON (
		   COALESCE(s.is_deleted, false) = false
		   AND (
		     (NULLIF(TRIM(m.scheme_id), '') IS NOT NULL AND s.scheme_id::text = TRIM(m.scheme_id))
		     OR (NULLIF(TRIM(m.scheme_id), '') IS NOT NULL AND s.internal_scheme_code = TRIM(m.scheme_id))
		     OR (NULLIF(TRIM(m.scheme_id), '') IS NOT NULL AND s.amfi_scheme_code = TRIM(m.scheme_id))
		   )
		)
		LEFT JOIN investment.masterfolio f ON (f.folio_id::text = m.folio_id OR f.folio_number = m.folio_id)
		LEFT JOIN investment.masterdemataccount d ON (
			d.demat_id::text = m.demat_id OR
			d.default_settlement_account = m.demat_id OR
			d.demat_account_number = m.demat_id
		)
		LEFT JOIN LATERAL (
			SELECT ans.nav_value, ans.nav_date
			FROM investment.amfi_nav_staging ans
			WHERE ans.nav_date IS NOT NULL
			  AND (
			    (COALESCE(s.amfi_scheme_code,'') <> '' AND ans.scheme_code::text = s.amfi_scheme_code)
			    OR (COALESCE(s.isin,'') <> '' AND (ans.isin_div_payout_growth = s.isin OR ans.isin_div_reinvestment = s.isin))
			  )
			ORDER BY ans.nav_date DESC
			LIMIT 1
		) nav ON true
		LEFT JOIN LATERAL (
			SELECT ai.* FROM uam.approval_instance ai
			WHERE ai.record_id = m.initiation_id::text
			  AND ai.module_code = 'INVESTMENT_MF'
			  AND ai.status = 'PENDING'
			  AND ai.is_deleted = false
			ORDER BY ai.submitted_at DESC, ai.instance_id DESC
			LIMIT 1
		) ai ON true
		LEFT JOIN LATERAL (
			SELECT aie.* FROM uam.approval_instance_eye aie
			WHERE aie.instance_id = ai.instance_id
			  AND aie.status = 'ACTIVE'
			ORDER BY aie.position ASC, aie.instance_eye_id ASC
			LIMIT 1
		) aie ON true
	`

	var (
		q    string
		args []interface{}
	)
	if len(ids) > 0 {
		q = baseSQL + " WHERE m.initiation_id = ANY($1) ORDER BY m.entity_name, m.initiation_id"
		args = []interface{}{ids}
	} else {
		args = []interface{}{}
		pos := 1
		where := " WHERE COALESCE(m.is_deleted, false) = false"
		if entityNames := suiteEntityNameRefs(ctx); len(entityNames) > 0 {
			where += fmt.Sprintf(" AND (COALESCE(m.entity_name,'') = '' OR m.entity_name = ANY($%d::text[]))", pos)
			args = append(args, entityNames)
			pos++
		}
		if schemeRefs := suiteMFSchemeRefs(ctx); len(schemeRefs) > 0 {
			where += fmt.Sprintf(" AND m.scheme_id = ANY($%d::text[])", pos)
			args = append(args, schemeRefs)
			pos++
		}
		if folioRefs := suiteMFFolioRefs(ctx); len(folioRefs) > 0 {
			where += fmt.Sprintf(" AND (COALESCE(m.folio_id,'') = '' OR m.folio_id = ANY($%d::text[]))", pos)
			args = append(args, folioRefs)
			pos++
		}
		if dematRefs := suiteMFDematRefs(ctx); len(dematRefs) > 0 {
			where += fmt.Sprintf(" AND (COALESCE(m.demat_id,'') = '' OR m.demat_id = ANY($%d::text[]))", pos)
			args = append(args, dematRefs)
		}
		q = baseSQL + where + " ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC"
	}

	rows, err := pgxPool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0, 1000)
	for rows.Next() {
		vals, _ := rows.Values()
		rec := make(map[string]interface{}, len(fields))
		for i, f := range fields {
			if vals[i] == nil {
				rec[string(f.Name)] = ""
			} else if t, ok := vals[i].(time.Time); ok {
				rec[string(f.Name)] = t.Format(constants.DateTimeFormat)
			} else {
				rec[string(f.Name)] = vals[i]
			}
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// ---------------------------
// Helper functions
// ---------------------------

func parseCashFlowCategoryFile(file multipart.File, ext string) ([][]string, error) {
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(data)

	switch strings.ToLower(ext) {
	case ".csv", "csv":
		br := bufio.NewReader(r)
		peek, _ := br.Peek(1024)
		delimiter := ','
		if bytes.Contains(peek, []byte(";")) {
			delimiter = ';'
		} else if bytes.Contains(peek, []byte("\t")) {
			delimiter = '\t'
		}

		if len(peek) >= 3 && peek[0] == 0xEF && peek[1] == 0xBB && peek[2] == 0xBF {
			br.Discard(3)
		}

		csvr := csv.NewReader(br)
		csvr.Comma = delimiter
		csvr.TrimLeadingSpace = true
		csvr.FieldsPerRecord = -1 // allow variable length rows
		csvr.ReuseRecord = false

		records, err := csvr.ReadAll()
		if err != nil {
			return nil, err
		}

		// remove any empty rows
		clean := make([][]string, 0, len(records))
		for _, row := range records {
			if len(strings.Join(row, "")) == 0 {
				continue
			}
			clean = append(clean, row)
		}

		return clean, nil

	case ".xlsx", ".xls", "xlsx", "xls":
		f, err := excelize.OpenReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		sheet := f.GetSheetName(0)
		rows, err := f.GetRows(sheet)
		if err != nil {
			return nil, err
		}
		return rows, nil

	default:
		return nil, errors.New(constants.ErrUnsupportedFileType)
	}
}

func getFileExt(filename string) string {
	parts := strings.Split(filename, ".")
	if len(parts) > 1 {
		return strings.ToLower(parts[len(parts)-1])
	}
	return ""
}

func normalizeHeader(row []string) []string {
	normalized := make([]string, len(row))
	for i, h := range row {
		normalized[i] = strings.ToLower(strings.TrimSpace(h))
	}
	return normalized
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func defaultIfEmpty(val, defaultVal string) string {
	if strings.TrimSpace(val) == "" {
		return defaultVal
	}
	return val
}

// GetInitiationDetail returns full detail for a single investment initiation.
func GetInitiationDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			InitiationID string `json:"initiation_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.InitiationID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "initiation_id is required")
			return
		}
		rows, err := fetchInitiationRows(r.Context(), pgxPool, []string{req.InitiationID})
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		if len(rows) == 0 {
			api.RespondWithError(w, http.StatusNotFound, "initiation not found")
			return
		}
		approvalWorkflow := loadMFInitiationApprovalWorkflow(r.Context(), pgxPool, req.InitiationID, api.GetUserIDFromCtx(r.Context()))
		api.RespondWithPayload(w, true, "", map[string]interface{}{"data": rows[0], "approval_workflow": approvalWorkflow})
	}
}
