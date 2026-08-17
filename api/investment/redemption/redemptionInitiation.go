package redemption

import (
	"CimplrCorpSaas/api"
	approvalengine "CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/portfolio"
	"CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/ctxutil"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"
	"CimplrCorpSaas/internal/validation"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// submitMFRedemptionForApproval fires the approval-matrix engine for a
// newly created or edited MF redemption initiation, asynchronously.
// submittedByUserID must be the session's numeric user_id (hard FK to
// public.users(id) — never a display name or email).
func submitMFRedemptionForApproval(pool *pgxpool.Pool, redemptionID, entityName, submittedByUserID, actorEmail, txType string, amount *float64) {
	go func() {
		bgCtx := context.Background()
		var amt float64
		if amount != nil {
			amt = *amount
		}
		if _, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
			ModuleCode:       "INVESTMENT_MF",
			EntityCode:       entityName,
			TransactionType:  txType,
			RecordID:         redemptionID,
			RecordTable:      "investment.redemption_initiation",
			AuditTable:       "investment.auditactionredemption",
			AuditIDColumn:    "redemption_id",
			ActionType:       strings.TrimPrefix(txType, "MF_REDEMPTION_INITIATION_"),
			Amount:           amt,
			SubmittedBy:      submittedByUserID,
			SubmittedByEmail: actorEmail,
		}); err != nil {
			api.LogError("[MFRedemptionInitiation] approvalengine.CreateInstance failed for redemption %s (%s): %v", redemptionID, txType, err)
		}
	}()
}

// loadMFRedemptionApprovalWorkflow finds the most recent INVESTMENT_MF
// approval-matrix instance for this redemption initiation and returns its
// rich detail for the ApprovalWorkflowViewer UI. Self-heals if no instance
// exists yet but a PENDING% audit row does. auditactionredemption stores the
// requester's real email in requested_by for CREATE/EDIT/DELETE rows.
func loadMFRedemptionApprovalWorkflow(ctx context.Context, pool *pgxpool.Pool, redemptionID, viewerUserID string) interface{} {
	if redemptionID == "" {
		return nil
	}

	var instanceID string
	_ = pool.QueryRow(ctx, `
		SELECT instance_id
		FROM uam.approval_instance
		WHERE record_id = $1 AND module_code = 'INVESTMENT_MF' AND is_deleted = false
		ORDER BY submitted_at DESC LIMIT 1`, redemptionID,
	).Scan(&instanceID)

	if instanceID == "" {
		var pendingActionType, entityName, submittedByUserID, submittedByEmail string
		var byAmount sql.NullFloat64
		scanErr := pool.QueryRow(ctx, `
			SELECT a.actiontype, COALESCE(r.entity_name,''), COALESCE(u.id,''), COALESCE(a.requested_by,''),
			       r.by_amount
			FROM investment.auditactionredemption a
			JOIN investment.redemption_initiation r ON r.redemption_id = a.redemption_id
			LEFT JOIN public.users u ON u.email = a.requested_by
			WHERE a.redemption_id = $1
			  AND a.processing_status LIKE 'PENDING%'
			ORDER BY a.requested_at DESC LIMIT 1`, redemptionID,
		).Scan(&pendingActionType, &entityName, &submittedByUserID, &submittedByEmail, &byAmount)

		if scanErr == nil && pendingActionType != "" && submittedByUserID != "" {
			txType := map[string]string{
				"CREATE": "MF_REDEMPTION_INITIATION_CREATE",
				"EDIT":   "MF_REDEMPTION_INITIATION_EDIT",
				"DELETE": "MF_REDEMPTION_INITIATION_DELETE",
			}[pendingActionType]
			if txType == "" {
				txType = "MF_REDEMPTION_INITIATION_CREATE"
			}
			var amount float64
			if byAmount.Valid {
				amount = byAmount.Float64
			}
			newInstID, instErr := approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
				ModuleCode:       "INVESTMENT_MF",
				EntityCode:       entityName,
				TransactionType:  txType,
				RecordID:         redemptionID,
				RecordTable:      "investment.redemption_initiation",
				AuditTable:       "investment.auditactionredemption",
				AuditIDColumn:    "redemption_id",
				ActionType:       pendingActionType,
				Amount:           amount,
				SubmittedBy:      submittedByUserID,
				SubmittedByEmail: submittedByEmail,
			})
			if instErr != nil {
				api.LogError("[MFRedemptionInitiation] Self-heal CreateInstance for %s: %v", redemptionID, instErr)
			} else if newInstID != "" {
				instanceID = newInstID
				api.LogInfo("[MFRedemptionInitiation] Self-heal: created instance %s for redemption %s", newInstID, redemptionID)
			}
		} else if scanErr == nil && pendingActionType != "" {
			api.LogInfo("[MFRedemptionInitiation] Self-heal skipped for %s: requester email did not resolve to a unique user_id", redemptionID)
		}
	}

	if instanceID == "" {
		return nil
	}
	richDetail, richErr := approvalengine.GetRichInstanceDetail(ctx, pool, instanceID, viewerUserID)
	if richErr != nil {
		api.LogError("[MFRedemptionInitiation] GetRichInstanceDetail failed for instance=%s redemption=%s: %v", instanceID, redemptionID, richErr)
		return nil
	}
	return richDetail
}

const (
	errLoadRedemptionForPolicyCheck = ": failed to load redemption for policy check: "
	redemptionInitiationApprovePath = "/investment/redemption/initiation/approve"
)

// ---------------------------
// Request/Response Types
// ---------------------------

type CreateRedemptionRequestSingle struct {
	UserID            string  `json:"user_id"`
	FolioID           string  `json:"folio_id,omitempty"`
	DematID           string  `json:"demat_id,omitempty"`
	SchemeID          string  `json:"scheme_id"`
	EntityName        string  `json:"entity_name,omitempty"`
	ByAmount          float64 `json:"by_amount,omitempty"`
	ByUnits           float64 `json:"by_units,omitempty"`
	Method            string  `json:"method,omitempty"`
	TransactionDate   string  `json:"transaction_date,omitempty"`
	EstimatedProceeds float64 `json:"estimated_proceeds,omitempty"`
	GainLoss          float64 `json:"gain_loss,omitempty"`
	Status            string  `json:"status,omitempty"`
	CreditBankAccount string  `json:"credit_bank_account,omitempty"`
}

type UpdateRedemptionRequest struct {
	UserID       string                 `json:"user_id"`
	RedemptionID string                 `json:"redemption_id"`
	Fields       map[string]interface{} `json:"fields"`
	Reason       string                 `json:"reason"`
}

type GetRedemptionDetailRequest struct {
	UserID       string `json:"user_id,omitempty"`
	EntityName   string `json:"entity_name,omitempty"`
	RedemptionID string `json:"redemption_id"`
}

func redemptionScopeValues(rows []map[string]string, keys ...string) []string {
	seen := make(map[string]struct{})
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		for _, key := range keys {
			value := strings.TrimSpace(row[key])
			if value == "" {
				continue
			}
			lookup := strings.ToUpper(value)
			if _, ok := seen[lookup]; ok {
				continue
			}
			seen[lookup] = struct{}{}
			values = append(values, value)
		}
	}
	return values
}

func redemptionEntityNameRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return scope.EntityNames
}

func redemptionMFSchemeRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return redemptionScopeValues(scope.Schemes, "scheme_id", "scheme_name", "isin", "internal_scheme_code")
}

func redemptionMFFolioRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return redemptionScopeValues(scope.Folios, "folio_id", "folio_number")
}

func redemptionMFDematRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return redemptionScopeValues(scope.Demats, "demat_id", "demat_account_number")
}

// ---------------------------
// CreateRedemptionSingle
// ---------------------------

func CreateRedemptionSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateRedemptionRequestSingle
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.SchemeID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "scheme_id is required")
			return
		}
		if req.ByAmount <= 0 && req.ByUnits <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Either by_amount or by_units must be greater than 0")
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

		userEmail := api.GetUserNameFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		entityCode := req.EntityName
		if entityCode == "" {
			entityCode = req.SchemeID
		}
		schemeName, amcName := lookupRedemptionSchemeEnrichment(ctx, pgxPool, req.SchemeID)
		if !mfEnforce(ctx, w, r, pgxPool, enforceCtx{EventCode: common.TriggerPreCreate, HandlerName: "CreateRedemptionSingle",
			APIPath: "/investment/redemption/initiation/create", SubModule: mfSubRedemption, EntityCode: entityCode, Actor: userEmail},
			buildRedemptionInitiationPolicyFields(redemptionInitiationRow{
				FolioID:           req.FolioID,
				DematID:           req.DematID,
				SchemeID:          req.SchemeID,
				SchemeName:        schemeName,
				AMCName:           amcName,
				RequestedBy:       userEmail,
				RequestedDate:     time.Now().Format("2006-01-02"),
				TransactionDate:   req.TransactionDate,
				ByAmount:          &req.ByAmount,
				ByUnits:           &req.ByUnits,
				Method:            "FIFO",
				EntityName:        req.EntityName,
				EstimatedProceeds: &req.EstimatedProceeds,
				GainLoss:          &req.GainLoss,
			})) {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Auto-resolve folio_id / demat_id from active holdings when neither is provided.
		// Handles both folio-based (MF) and demat-based holdings.
		if req.FolioID == "" && req.DematID == "" {
			var resolvedFolioID, resolvedDematID string
			resolveErr := tx.QueryRow(ctx, `
				SELECT COALESCE(ot.folio_id, ''), COALESCE(ot.demat_id, '')
				FROM investment.onboard_transaction ot
				LEFT JOIN investment.masterscheme ms ON (
					COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))
				)
				WHERE LOWER(COALESCE(ot.transaction_type, '')) IN ('buy', 'purchase', 'subscription')
				  AND (
					ms.scheme_id::text = $1 OR
					ms.internal_scheme_code = $1 OR ms.amfi_scheme_code = $1 OR
					ot.scheme_id = $1 OR ot.scheme_internal_code = $1
				  )
				  AND ($2::text IS NULL OR ot.entity_name = $2)
				  AND (COALESCE(ot.folio_id, '') <> '' OR COALESCE(ot.demat_id, '') <> '')
				  AND (ot.units - COALESCE(ot.blocked_units, 0)) > 0
				ORDER BY
					CASE WHEN COALESCE(ot.folio_id, '') <> '' THEN 0 ELSE 1 END,
					ot.transaction_date DESC
				LIMIT 1
			`, req.SchemeID, nullIfEmptyString(req.EntityName)).Scan(&resolvedFolioID, &resolvedDematID)
			if resolveErr != nil || (resolvedFolioID == "" && resolvedDematID == "") {
				api.RespondWithError(w, http.StatusBadRequest,
					"No active holding found for scheme "+req.SchemeID+". Provide folio_id or demat_id explicitly.")
				return
			}
			if resolvedFolioID != "" {
				req.FolioID = resolvedFolioID
			} else {
				req.DematID = resolvedDematID
			}
		}

		// Method will be fetched from masterscheme at query time; do not store in redemption_initiation

		// Calculate units to block based on by_amount or by_units
		var unitsToBlock float64
		if req.ByUnits > 0 {
			// Direct units provided
			unitsToBlock = req.ByUnits
		} else if req.ByAmount > 0 {
			// Calculate units from amount using latest NAV
			var latestNAV float64
			navQuery := `
				SELECT ans.nav_value
				FROM investment.amfi_nav_staging ans
				LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ms.amfi_scheme_code = ans.scheme_code::text)
				WHERE (
					ms.scheme_id = $1 OR
										ms.internal_scheme_code = $1 OR
					ms.amfi_scheme_code = $1
				)
				ORDER BY ans.nav_date DESC, ans.file_date DESC
				LIMIT 1
			`
			if err := tx.QueryRow(ctx, navQuery, req.SchemeID).Scan(&latestNAV); err != nil || latestNAV == 0 {
				api.RespondWithError(w, http.StatusInternalServerError, "Unable to fetch NAV for amount-based redemption: "+err.Error())
				return
			}
			unitsToBlock = req.ByAmount / latestNAV
		}

		// Update blocked_units in onboard_transaction for the holding
		// CRITICAL: Use entity_name filter to prevent cross-entity blocking when folio_number is non-unique (e.g., "1")
		if unitsToBlock > 0 {
			updateBlockedUnitsQuery := `
				WITH target_transactions AS (
					SELECT 
						ot.id,
						ot.units,
						COALESCE(ot.blocked_units, 0) AS current_blocked,
						ot.transaction_date,
						ROW_NUMBER() OVER (
							ORDER BY 
								CASE WHEN $4 = 'FIFO' THEN ot.transaction_date END ASC,
								CASE WHEN $4 = 'LIFO' THEN ot.transaction_date END DESC,
								ot.id ASC
						) AS row_num
					FROM investment.onboard_transaction ot
					LEFT JOIN investment.masterscheme ms ON (
						COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))
					)
					WHERE 
						LOWER(COALESCE(ot.transaction_type, '')) IN ('buy', 'purchase', 'subscription')
						AND (
							ms.scheme_id = $1 OR
														ms.internal_scheme_code = $1 OR
														ot.scheme_id = $1 OR
							ot.scheme_internal_code = $1
						)
						AND (
							-- When no folio/demat supplied, match on scheme+entity alone.
							($2::text IS NULL AND $3::text IS NULL)
							OR ($2::text IS NOT NULL AND ot.folio_id = $2)
							OR ($3::text IS NOT NULL AND ot.demat_id = $3)
						)
						AND ($6::text IS NULL OR ot.entity_name = $6)
						AND (ot.units - COALESCE(ot.blocked_units, 0)) > 0
				),
				blocking_allocation AS (
					SELECT
						id,
						units,
						current_blocked,
						LEAST(
							units - current_blocked,
							$5 - COALESCE(SUM(LEAST(units - current_blocked, $5)) OVER (
								ORDER BY row_num
								ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
							), 0)
						) AS units_to_block_here
					FROM target_transactions
				)
				UPDATE investment.onboard_transaction ot
				SET blocked_units = COALESCE(ot.blocked_units, 0) + ba.units_to_block_here
				FROM blocking_allocation ba
				WHERE ot.id = ba.id AND ba.units_to_block_here > 0
			`
			if _, err := tx.Exec(ctx, updateBlockedUnitsQuery,
				req.SchemeID,
				nullIfEmptyString(req.FolioID),
				nullIfEmptyString(req.DematID),
				"FIFO", // default for unit blocking ORDER BY
				unitsToBlock,
				nullIfEmptyString(req.EntityName),
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to block units: "+err.Error())
				return
			}
		}

		insertQ := `
		    INSERT INTO investment.redemption_initiation (
			    folio_id, demat_id, scheme_id, requested_by, requested_date, transaction_date,
			    by_amount, by_units, entity_name, old_entity_name, estimated_proceeds, gain_loss
		    ) VALUES ($1, $2, $3, $4, now()::date, $5, $6, $7, $8, $9, $10, $11)
		    RETURNING redemption_id
		`
		var redemptionID string
		// requested_by is set from session email; requested_date uses current date
		if err := tx.QueryRow(ctx, insertQ,
			nullIfEmptyString(req.FolioID),
			nullIfEmptyString(req.DematID),
			req.SchemeID,
			userEmail,
			nullIfEmptyString(req.TransactionDate),
			nullIfZeroFloat(req.ByAmount),
			nullIfZeroFloat(req.ByUnits),
			nullIfEmptyString(req.EntityName),
			nil,
			nullIfZeroFloat(req.EstimatedProceeds),
			nullIfZeroFloat(req.GainLoss),
		).Scan(&redemptionID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}

		// Create audit trail
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionredemption (redemption_id, actiontype, processing_status, requested_by, requested_at, requested_ip)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now(), $3)
		`, redemptionID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		// Fire approval-matrix engine instance (async, no-op if engine disabled or no matrix)
		var amountToPass *float64
		if req.ByAmount > 0 {
			amountToPass = &req.ByAmount
		} else if req.EstimatedProceeds > 0 {
			amountToPass = &req.EstimatedProceeds
		}
		submitMFRedemptionForApproval(pgxPool, redemptionID, req.EntityName, req.UserID, userEmail, "MF_REDEMPTION_INITIATION_CREATE", amountToPass)

		correlationID := redemptionID
		go func() {
			payload := BuildRedemptionInitiationNotifPayload(ctx, pgxPool, []string{redemptionID}, constants.AuditActionCreate, userEmail)
			catalog.TriggerNotification(ctx, pgxPool, "/investment/redemption/initiation/create", correlationID, payload.ToMap())
			dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_MF", "MF_REDEMPTION", "POST_CREATE", []string{redemptionID}, userEmail)
		}()

		api.RespondWithPayload(w, true, "", map[string]any{
			"redemption_id": redemptionID,
			"requested":     userEmail,
		})
	}
}

// ---------------------------
// CreateRedemptionBulk
// ---------------------------

func CreateRedemptionBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				FolioID           string  `json:"folio_id,omitempty"`
				DematID           string  `json:"demat_id,omitempty"`
				SchemeID          string  `json:"scheme_id"`
				EntityName        string  `json:"entity_name,omitempty"`
				ByAmount          float64 `json:"by_amount,omitempty"`
				ByUnits           float64 `json:"by_units,omitempty"`
				Method            string  `json:"method,omitempty"`
				TransactionDate   string  `json:"transaction_date,omitempty"`
				EstimatedProceeds float64 `json:"estimated_proceeds,omitempty"`
				GainLoss          float64 `json:"gain_loss,omitempty"`
				Status            string  `json:"status,omitempty"`
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

		userEmail := api.GetUserNameFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			// Validate
			if strings.TrimSpace(row.SchemeID) == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "scheme_id is required"})
				continue
			}
			if row.ByAmount <= 0 && row.ByUnits <= 0 {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Either by_amount or by_units must be > 0"})
				continue
			}
			if errMsg := validation.ValidateMFMasterReferences(ctx, map[string]interface{}{
				"entity_name": row.EntityName,
				"scheme_id":   row.SchemeID,
				"folio_id":    row.FolioID,
				"demat_id":    row.DematID,
			}); errMsg != "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: errMsg})
				continue
			}

			entityCode := row.EntityName
			if entityCode == "" {
				entityCode = row.SchemeID
			}
			bulkSchemeName, bulkAMCName := lookupRedemptionSchemeEnrichment(ctx, pgxPool, row.SchemeID)
			if ok, pmsg := mfEnforceInline(ctx, r, pgxPool, enforceCtx{EventCode: common.TriggerPreCreate, HandlerName: "CreateRedemptionBulk",
				APIPath: "/investment/redemption/initiation/create-bulk", SubModule: mfSubRedemption, EntityCode: entityCode, Actor: userEmail},
				buildRedemptionInitiationPolicyFields(redemptionInitiationRow{
					FolioID:           row.FolioID,
					DematID:           row.DematID,
					SchemeID:          row.SchemeID,
					SchemeName:        bulkSchemeName,
					AMCName:           bulkAMCName,
					RequestedBy:       userEmail,
					RequestedDate:     time.Now().Format("2006-01-02"),
					TransactionDate:   row.TransactionDate,
					ByAmount:          &row.ByAmount,
					ByUnits:           &row.ByUnits,
					Method:            "FIFO",
					EntityName:        row.EntityName,
					EstimatedProceeds: &row.EstimatedProceeds,
					GainLoss:          &row.GainLoss,
				})); !ok {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: pmsg})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxBeginFailed + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			// Auto-resolve folio_id / demat_id from active holdings when neither is provided.
			if row.FolioID == "" && row.DematID == "" {
				var resolvedFolioID, resolvedDematID string
				resolveErr := tx.QueryRow(ctx, `
					SELECT COALESCE(ot.folio_id, ''), COALESCE(ot.demat_id, '')
					FROM investment.onboard_transaction ot
					LEFT JOIN investment.masterscheme ms ON (
						COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))
					)
					WHERE LOWER(COALESCE(ot.transaction_type, '')) IN ('buy', 'purchase', 'subscription')
					  AND (
						ms.scheme_id::text = $1 OR
						ms.internal_scheme_code = $1 OR ms.amfi_scheme_code = $1 OR
						ot.scheme_id = $1 OR ot.scheme_internal_code = $1
					  )
					  AND ($2::text IS NULL OR ot.entity_name = $2)
					  AND (COALESCE(ot.folio_id, '') <> '' OR COALESCE(ot.demat_id, '') <> '')
					  AND (ot.units - COALESCE(ot.blocked_units, 0)) > 0
					ORDER BY
						CASE WHEN COALESCE(ot.folio_id, '') <> '' THEN 0 ELSE 1 END,
						ot.transaction_date DESC
					LIMIT 1
				`, row.SchemeID, nullIfEmptyString(row.EntityName)).Scan(&resolvedFolioID, &resolvedDematID)
				if resolveErr != nil || (resolvedFolioID == "" && resolvedDematID == "") {
					tx.Rollback(ctx)
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "No active holding found for scheme " + row.SchemeID + ". Provide folio_id or demat_id explicitly."})
					continue
				}
				if resolvedFolioID != "" {
					row.FolioID = resolvedFolioID
				} else {
					row.DematID = resolvedDematID
				}
			}

			// Method will be fetched from masterscheme at query time

			// Calculate units to block
			var unitsToBlock float64
			if row.ByUnits > 0 {
				unitsToBlock = row.ByUnits
			} else if row.ByAmount > 0 {
				var latestNAV float64
				navQuery := `
					SELECT ans.nav_value
					FROM investment.amfi_nav_staging ans
					LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ms.amfi_scheme_code = ans.scheme_code::text)
					WHERE (
						ms.scheme_id = $1 OR
												ms.internal_scheme_code = $1 OR
						ms.amfi_scheme_code = $1
					)
					ORDER BY ans.nav_date DESC, ans.file_date DESC
					LIMIT 1
				`
				if err := tx.QueryRow(ctx, navQuery, row.SchemeID).Scan(&latestNAV); err != nil || latestNAV == 0 {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Unable to fetch NAV: " + err.Error()})
					continue
				}
				unitsToBlock = row.ByAmount / latestNAV
			}

			// Update blocked_units with entity scoping
			if unitsToBlock > 0 {
				updateBlockedUnitsQuery := `
					WITH target_transactions AS (
						SELECT 
							ot.id,
							ot.units,
							COALESCE(ot.blocked_units, 0) AS current_blocked,
							ot.transaction_date,
							ROW_NUMBER() OVER (
								ORDER BY 
									CASE WHEN $4 = 'FIFO' THEN ot.transaction_date END ASC,
									CASE WHEN $4 = 'LIFO' THEN ot.transaction_date END DESC,
									ot.id ASC
							) AS row_num
						FROM investment.onboard_transaction ot
						LEFT JOIN investment.masterscheme ms ON (
							COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))
						)
						WHERE 
							LOWER(COALESCE(ot.transaction_type, '')) IN ('buy', 'purchase', 'subscription')
							AND (
								ms.scheme_id = $1 OR
																ms.internal_scheme_code = $1 OR
																ot.scheme_id = $1 OR
								ot.scheme_internal_code = $1
							)
							AND (
								-- When no folio/demat supplied, match on scheme+entity alone.
								($2::text IS NULL AND $3::text IS NULL)
								OR ($2::text IS NOT NULL AND ot.folio_id = $2)
								OR ($3::text IS NOT NULL AND ot.demat_id = $3)
							)
							AND ($6::text IS NULL OR ot.entity_name = $6)
							AND (ot.units - COALESCE(ot.blocked_units, 0)) > 0
					),
					blocking_allocation AS (
						SELECT 
							id,
							units,
							current_blocked,
							LEAST(
								units - current_blocked,
								$5 - COALESCE(SUM(LEAST(units - current_blocked, $5)) OVER (
									ORDER BY row_num
									ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
								), 0)
							) AS units_to_block_here
						FROM target_transactions
					)
					UPDATE investment.onboard_transaction ot
					SET blocked_units = COALESCE(ot.blocked_units, 0) + ba.units_to_block_here
					FROM blocking_allocation ba
					WHERE ot.id = ba.id AND ba.units_to_block_here > 0
				`
				if _, err := tx.Exec(ctx, updateBlockedUnitsQuery,
					row.SchemeID,
					nullIfEmptyString(row.FolioID),
					nullIfEmptyString(row.DematID),
					"FIFO", // default for unit blocking ORDER BY
					unitsToBlock,
					nullIfEmptyString(row.EntityName),
				); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Failed to block units: " + err.Error()})
					continue
				}
			}

			var redemptionID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.redemption_initiation (
						folio_id, demat_id, scheme_id, requested_by, requested_date, transaction_date,
						by_amount, by_units, entity_name, estimated_proceeds, gain_loss
				) VALUES ($1, $2, $3, $4, now()::date, $5, $6, $7, $8, $9, $10)
				RETURNING redemption_id
			`, nullIfEmptyString(row.FolioID), nullIfEmptyString(row.DematID), row.SchemeID, userEmail,
				nullIfEmptyString(row.TransactionDate), nullIfZeroFloat(row.ByAmount), nullIfZeroFloat(row.ByUnits), nullIfEmptyString(row.EntityName), nullIfZeroFloat(row.EstimatedProceeds), nullIfZeroFloat(row.GainLoss)).Scan(&redemptionID); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Insert failed: " + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionredemption (redemption_id, actiontype, processing_status, requested_by, requested_at, requested_ip)
				VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now(), $3)
			`, redemptionID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrAuditInsertFailed + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailedCapitalized + err.Error()})
				continue
			}

			var amountToPass *float64
			if row.ByAmount > 0 {
				amountToPass = &row.ByAmount
			} else if row.EstimatedProceeds > 0 {
				amountToPass = &row.EstimatedProceeds
			}
			submitMFRedemptionForApproval(pgxPool, redemptionID, row.EntityName, req.UserID, userEmail, "MF_REDEMPTION_INITIATION_CREATE", amountToPass)

			dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_MF", "MF_REDEMPTION", "POST_CREATE", []string{redemptionID}, userEmail)

			results = append(results, map[string]interface{}{
				constants.ValueSuccess: true,
				"redemption_id":        redemptionID,
			})
		}

		createdIDs := make([]string, 0, len(results))
		for _, res := range results {
			if ok, _ := res[constants.ValueSuccess].(bool); ok {
				if id, _ := res["redemption_id"].(string); id != "" {
					createdIDs = append(createdIDs, id)
				}
			}
		}
		if len(createdIDs) > 0 {
			go func() {
				payload := BuildRedemptionInitiationNotifPayload(ctx, pgxPool, createdIDs, constants.AuditActionCreate, userEmail)
				catalog.TriggerNotification(ctx, pgxPool, "/investment/redemption/initiation/create-bulk", createdIDs[0], payload.ToMap())
			}()
		}
		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// UpdateRedemption
// ---------------------------

func UpdateRedemption(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateRedemptionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.RedemptionID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "redemption_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFieldsToUpdateUser)
			return
		}

		ctx := r.Context()
		if errMsg := validation.ValidateMFMasterReferences(ctx, req.Fields); errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}

		userEmail := api.GetUserNameFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		existingRow, err := loadRedemptionInitiationRow(ctx, pgxPool, req.RedemptionID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to load redemption for policy check: "+err.Error())
			return
		}
		mergedRow := applyRedemptionInitiationEdits(existingRow, req.Fields)
		if !mfEnforce(ctx, w, r, pgxPool, enforceCtx{EventCode: common.TriggerPreEdit, HandlerName: "UpdateRedemption",
			APIPath: "/investment/redemption/initiation/update", SubModule: mfSubRedemption, EntityCode: req.RedemptionID, Actor: userEmail},
			buildRedemptionInitiationPolicyFields(mergedRow)) {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Fetch existing values (include transaction_date so scans align with fieldPairs)
		sel := `
			SELECT folio_id, demat_id, scheme_id, requested_by, requested_date, transaction_date,
				   by_amount, by_units, estimated_proceeds, gain_loss
			FROM investment.redemption_initiation WHERE redemption_id=$1 FOR UPDATE`
		var oldVals [10]interface{}
		if err := tx.QueryRow(ctx, sel, req.RedemptionID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4], &oldVals[5],
			&oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"folio_id":           0,
			"demat_id":           1,
			"scheme_id":          2,
			"requested_by":       3,
			"requested_date":     4,
			"transaction_date":   5,
			"by_amount":          6,
			"by_units":           7,
			"estimated_proceeds": 8,
			"gain_loss":          9,
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

		q := fmt.Sprintf("UPDATE investment.redemption_initiation SET %s, updated_at=now() WHERE redemption_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.RedemptionID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
			return
		}

		// Audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionredemption (redemption_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
			VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now(), $4)
		`, req.RedemptionID, req.Reason, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		go func() {
			payload := BuildRedemptionInitiationNotifPayload(ctx, pgxPool, []string{req.RedemptionID}, "UPDATE", userEmail)
			catalog.TriggerNotification(ctx, pgxPool, "/investment/redemption/initiation/update", req.RedemptionID, payload.ToMap())
		}()

		api.RespondWithPayload(w, true, "", map[string]any{
			"redemption_id": req.RedemptionID,
			"requested":     userEmail,
		})
	}
}

// ---------------------------
// UpdateRedemptionBulk
// ---------------------------

func UpdateRedemptionBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				RedemptionID string                 `json:"redemption_id"`
				Fields       map[string]interface{} `json:"fields"`
				Reason       string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		userEmail := api.GetUserNameFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if row.RedemptionID == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "redemption_id missing"})
				continue
			}
			if errMsg := validation.ValidateMFMasterReferences(ctx, row.Fields); errMsg != "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: errMsg})
				continue
			}

			existingRow, err := loadRedemptionInitiationRow(ctx, pgxPool, row.RedemptionID)
			if err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: "failed to load redemption for policy check: " + err.Error(),
				})
				continue
			}
			mergedRow := applyRedemptionInitiationEdits(existingRow, row.Fields)
			if ok, pmsg := mfEnforceInline(ctx, r, pgxPool, enforceCtx{EventCode: common.TriggerPreEdit, HandlerName: "UpdateRedemptionBulk",
				APIPath: "/investment/redemption/initiation/update-bulk", SubModule: mfSubRedemption, EntityCode: row.RedemptionID, Actor: userEmail},
				buildRedemptionInitiationPolicyFields(mergedRow)); !ok {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: pmsg,
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: constants.ErrTxBeginFailedCapitalized + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			sel := `
				SELECT folio_id, demat_id, scheme_id, requested_by, requested_date, transaction_date,
					   by_amount, by_units, estimated_proceeds, gain_loss
				FROM investment.redemption_initiation WHERE redemption_id=$1 FOR UPDATE`
			var oldVals [10]interface{}
			if err := tx.QueryRow(ctx, sel, row.RedemptionID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4], &oldVals[5],
				&oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9],
			); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"folio_id":           0,
				"demat_id":           1,
				"scheme_id":          2,
				"requested_by":       3,
				"requested_date":     4,
				"transaction_date":   5,
				"by_amount":          6,
				"by_units":           7,
				"estimated_proceeds": 8,
				"gain_loss":          9,
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
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: "No valid fields"})
				continue
			}

			q := fmt.Sprintf("UPDATE investment.redemption_initiation SET %s, updated_at=now() WHERE redemption_id=$%d", strings.Join(sets, ", "), pos)
			args = append(args, row.RedemptionID)

			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: constants.ErrUpdateFailed + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionredemption (redemption_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
				VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now(), $4)
			`, row.RedemptionID, row.Reason, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: constants.ErrAuditInsertFailed + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: constants.ErrCommitFailed + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{constants.ValueSuccess: true, "redemption_id": row.RedemptionID, "requested": userEmail})
		}

		updatedIDs := make([]string, 0, len(results))
		for _, res := range results {
			if ok, _ := res[constants.ValueSuccess].(bool); ok {
				if id, _ := res["redemption_id"].(string); id != "" {
					updatedIDs = append(updatedIDs, id)
				}
			}
		}
		if len(updatedIDs) > 0 {
			go func() {
				payload := BuildRedemptionInitiationNotifPayload(ctx, pgxPool, updatedIDs, "UPDATE", userEmail)
				catalog.TriggerNotification(ctx, pgxPool, "/investment/redemption/initiation/update-bulk", updatedIDs[0], payload.ToMap())
			}()
		}
		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// DeleteRedemption
// ---------------------------

func DeleteRedemption(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			RedemptionIDs []string `json:"redemption_ids"`
			Reason        string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if len(req.RedemptionIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "redemption_ids required")
			return
		}

		requestedBy := api.GetUserNameFromCtx(r.Context())
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		for _, id := range req.RedemptionIDs {
			row, rowErr := loadRedemptionInitiationRow(ctx, pgxPool, id)
			if rowErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, id+errLoadRedemptionForPolicyCheck+rowErr.Error())
				return
			}
			if ok, pmsg := mfEnforceInline(ctx, r, pgxPool, enforceCtx{EventCode: common.TriggerPreDelete, HandlerName: "DeleteRedemption",
				APIPath: "/investment/redemption/initiation/delete", SubModule: mfSubRedemption, EntityCode: id, Actor: requestedBy},
				buildRedemptionInitiationPolicyFields(row)); !ok {
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

		for _, id := range req.RedemptionIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionredemption (redemption_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
				VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now(), $4)
			`, id, req.Reason, api.SystemIfBlank(requestedBy), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		// ── Approval-matrix engine: cancel stale instances and create DELETE instances.
		for _, id := range req.RedemptionIDs {
			if err := approvalengine.CancelPendingInstances(context.Background(), pgxPool, "INVESTMENT_MF", id, requestedBy); err != nil {
				api.LogError("[MFRedemptionInitiation] CancelPendingInstances for delete failed for %s: %v", id, err)
			}
			redemptionRow, _ := loadRedemptionInitiationRow(context.Background(), pgxPool, id)
			var amountToPass *float64
			if redemptionRow.ByAmount != nil && *redemptionRow.ByAmount > 0 {
				amountToPass = redemptionRow.ByAmount
			} else if redemptionRow.EstimatedProceeds != nil && *redemptionRow.EstimatedProceeds > 0 {
				amountToPass = redemptionRow.EstimatedProceeds
			}
			submitMFRedemptionForApproval(pgxPool, id, redemptionRow.EntityName, req.UserID, requestedBy, "MF_REDEMPTION_INITIATION_DELETE", amountToPass)
		}

		go func() {
			payload := BuildRedemptionInitiationNotifPayload(ctx, pgxPool, req.RedemptionIDs, "DELETE_REQUEST", requestedBy)
			corID := ""
			if len(req.RedemptionIDs) > 0 {
				corID = req.RedemptionIDs[0]
			}
			catalog.TriggerNotification(ctx, pgxPool, "/investment/redemption/initiation/delete", corID, payload.ToMap())
		}()

		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.RedemptionIDs})
	}
}

// ---------------------------
// BulkApproveRedemptionActions
// ---------------------------

func BulkApproveRedemptionActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			RedemptionIDs []string `json:"redemption_ids"`
			Comment       string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		checkerBy := api.GetUserNameFromCtx(r.Context())
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		for _, id := range req.RedemptionIDs {
			row, rowErr := loadRedemptionInitiationRow(ctx, pgxPool, id)
			if rowErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, id+errLoadRedemptionForPolicyCheck+rowErr.Error())
				return
			}
			if ok, pmsg := mfEnforceInline(ctx, r, pgxPool, enforceCtx{EventCode: common.TriggerPreApprove, HandlerName: "BulkApproveRedemptionActions",
				APIPath: redemptionInitiationApprovePath, SubModule: mfSubRedemption, EntityCode: id, Actor: checkerBy},
				buildRedemptionInitiationPolicyFields(row)); !ok {
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
			SELECT DISTINCT ON (redemption_id) action_id, redemption_id, actiontype, processing_status
			FROM investment.auditactionredemption
			WHERE redemption_id = ANY($1)
			  AND UPPER(COALESCE(actiontype, '')) <> 'UPLOAD_FILE'
			ORDER BY redemption_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.RedemptionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var toApprove []string
		var toApproveRedemptionIDs []string
		var editRedemptionIDs []string
		var toDeleteActionIDs []string
		var deleteMasterIDs []string

		for rows.Next() {
			var aid, rid, atype, pstatus string
			if err := rows.Scan(&aid, &rid, &atype, &pstatus); err != nil {
				continue
			}
			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			if ps == constants.StatusApproved {
				continue
			}
			if ps == constants.StatusPendingDeleteApproval {
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteMasterIDs = append(deleteMasterIDs, rid)
				continue
			}
			if ps == constants.StatusPendingApproval || ps == constants.StatusPendingEditApproval {
				toApprove = append(toApprove, aid)
				toApproveRedemptionIDs = append(toApproveRedemptionIDs, rid)
				if ps == constants.StatusPendingEditApproval {
					editRedemptionIDs = append(editRedemptionIDs, rid)
				}
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrNoApprovableActions, map[string]any{
				"approved_action_ids": []string{},
				"deleted_redemptions": []string{},
			})
			return
		}

		// ── Approval-matrix engine: handle engine-managed records first.
		// Records the engine handled (Acted==true) are skipped in legacy stamp below.
		engineActed := map[string]bool{}
		for _, rid := range req.RedemptionIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{
				ModuleCode: "INVESTMENT_MF", RecordID: rid,
				UserID: req.UserID, UserEmail: checkerBy,
				Action: approvalengine.ActionApproved, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[MFRedemptionInitiation] ActOnPendingOrDiagnose approve failed for %s: %v", rid, actionErr)
				continue
			}
			if actionRes.Acted {
				engineActed[rid] = true
			} else if actionRes.CancelledStale {
				api.LogInfo("[MFRedemptionInitiation] cancelled stale instance for %s", rid)
			} else if actionRes.Reason != "" {
				api.LogInfo("[MFRedemptionInitiation] engine skipped %s: %s", rid, actionRes.Reason)
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
		toApproveRedemptionIDs = filterEA(toApproveRedemptionIDs)
		toDeleteActionIDs = filterEA(toDeleteActionIDs)
		deleteMasterIDs = filterEA(deleteMasterIDs)

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionredemption
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				WHERE action_id = ANY($4)
			`, api.SystemIfBlank(checkerBy), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), toApprove); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "approve failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionredemption
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				WHERE action_id = ANY($4)
			`, api.SystemIfBlank(checkerBy), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), toDeleteActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete action update failed: "+err.Error())
				return
			}

			// Release blocked units for deleted redemptions
			for _, rid := range deleteMasterIDs {
				// Fetch redemption details to determine which units to unblock
				var folioID, dematID, schemeID, entityName sql.NullString
				var byUnits sql.NullFloat64
				var method string

				if err := tx.QueryRow(ctx, `
					SELECT ri.folio_id, ri.demat_id, ri.scheme_id, ri.by_units, COALESCE(ms.method, 'FIFO') AS method, ri.entity_name
					FROM investment.redemption_initiation ri
					LEFT JOIN investment.masterscheme ms ON (
						ms.scheme_id = ri.scheme_id OR
						ms.internal_scheme_code = ri.scheme_id OR
						ms.amfi_scheme_code = ri.scheme_id
					)
					WHERE ri.redemption_id = $1
				`, rid).Scan(&folioID, &dematID, &schemeID, &byUnits, &method, &entityName); err != nil {
					continue // Skip if unable to fetch details
				}

				if !byUnits.Valid || byUnits.Float64 <= 0 {
					continue // Nothing to unblock
				}

				// Release blocked units with entity scoping to prevent cross-entity leakage
				unblockQuery := `
					WITH target_transactions AS (
						SELECT 
							ot.id,
							COALESCE(ot.blocked_units, 0) AS current_blocked,
							ot.transaction_date,
							ROW_NUMBER() OVER (
								ORDER BY 
									CASE WHEN $4 = 'FIFO' THEN ot.transaction_date END ASC,
									CASE WHEN $4 = 'LIFO' THEN ot.transaction_date END DESC,
									ot.id ASC
							) AS row_num
						FROM investment.onboard_transaction ot
						LEFT JOIN investment.masterscheme ms ON (
							COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))
						)
						WHERE 
							LOWER(COALESCE(ot.transaction_type, '')) IN ('buy', 'purchase', 'subscription')
							AND (
								ms.scheme_id = $1 OR
																ms.internal_scheme_code = $1 OR
																ot.scheme_id = $1 OR
								ot.scheme_internal_code = $1
							)
							AND (
								($2::text IS NOT NULL AND ot.folio_id = $2) OR
								($3::text IS NOT NULL AND ot.demat_id = $3)
							)
							AND ($6::text IS NULL OR ot.entity_name = $6)
							AND COALESCE(ot.blocked_units, 0) > 0
					),
					unblocking_allocation AS (
						SELECT 
							id,
							current_blocked,
							LEAST(
								current_blocked,
								$5 - COALESCE(SUM(LEAST(current_blocked, $5)) OVER (
									ORDER BY row_num
									ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
								), 0)
							) AS units_to_unblock_here
						FROM target_transactions
					)
					UPDATE investment.onboard_transaction ot
					SET blocked_units = GREATEST(0, COALESCE(ot.blocked_units, 0) - ua.units_to_unblock_here)
					FROM unblocking_allocation ua
					WHERE ot.id = ua.id AND ua.units_to_unblock_here > 0
				`

				var folioIDStr, dematIDStr, entityNameStr *string
				if folioID.Valid {
					folioIDStr = &folioID.String
				}
				if dematID.Valid {
					dematIDStr = &dematID.String
				}
				if entityName.Valid {
					entityNameStr = &entityName.String
				}

				if _, err := tx.Exec(ctx, unblockQuery,
					schemeID.String,
					nullIfEmptyStringPtr(folioIDStr),
					nullIfEmptyStringPtr(dematIDStr),
					method,
					byUnits.Float64,
					nullIfEmptyStringPtr(entityNameStr),
				); err != nil {
					// Log error but continue with deletion
				}
			}

			if _, err := tx.Exec(ctx, `
				UPDATE investment.redemption_initiation
				SET is_deleted=true, updated_at=now()
				WHERE redemption_id = ANY($1)
			`, deleteMasterIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete redemption failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		if len(toApprove) > 0 {
			ids := append([]string{}, toApproveRedemptionIDs...)
			editSet := make(map[string]struct{}, len(editRedemptionIDs))
			for _, id := range editRedemptionIDs {
				editSet[id] = struct{}{}
			}
			go func() {
				payload := BuildRedemptionInitiationNotifPayload(ctx, pgxPool, ids, constants.AuditActionApprove, checkerBy)
				catalog.TriggerNotification(ctx, pgxPool, redemptionInitiationApprovePath, ids[0], payload.ToMap())
				for _, id := range ids {
					trigger := "POST_APPROVE"
					if _, isEdit := editSet[id]; isEdit {
						trigger = "POST_EDIT"
					}
					dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_MF", "MF_REDEMPTION", trigger, []string{id}, checkerBy)
				}
			}()
		}
		if len(deleteMasterIDs) > 0 {
			go func() {
				payload := BuildRedemptionInitiationNotifPayload(ctx, pgxPool, deleteMasterIDs, constants.AuditActionDelete, checkerBy)
				catalog.TriggerNotification(ctx, pgxPool, redemptionInitiationApprovePath, deleteMasterIDs[0], payload.ToMap())
				for _, id := range deleteMasterIDs {
					dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_MF", "MF_REDEMPTION", "POST_DELETE", []string{id}, checkerBy)
				}
			}()
		}
		api.RespondWithPayload(w, true, "", map[string]any{
			"approved_action_ids": toApprove,
			"deleted_redemptions": deleteMasterIDs,
		})
	}
}

// ---------------------------
// BulkRejectRedemptionActions
// ---------------------------

func BulkRejectRedemptionActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			RedemptionIDs []string `json:"redemption_ids"`
			Comment       string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		checkerBy := api.GetUserNameFromCtx(r.Context())
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
			SELECT DISTINCT ON (redemption_id) action_id, redemption_id, processing_status
			FROM investment.auditactionredemption
			WHERE redemption_id = ANY($1)
			  AND UPPER(COALESCE(actiontype, '')) <> 'UPLOAD_FILE'
			ORDER BY redemption_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.RedemptionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := []string{}
		cannotReject := []string{}
		found := map[string]bool{}
		for rows.Next() {
			var aid, rid, ps string
			if err := rows.Scan(&aid, &rid, &ps); err != nil {
				continue
			}
			found[rid] = true
			if strings.ToUpper(strings.TrimSpace(ps)) == constants.StatusApproved {
				cannotReject = append(cannotReject, rid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		missing := []string{}
		for _, id := range req.RedemptionIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for redemption_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved redemption_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		for _, id := range req.RedemptionIDs {
			row, rowErr := loadRedemptionInitiationRow(ctx, pgxPool, id)
			if rowErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, id+errLoadRedemptionForPolicyCheck+rowErr.Error())
				return
			}
			if ok, pmsg := mfEnforceInline(ctx, r, pgxPool, enforceCtx{EventCode: common.TriggerPreReject, HandlerName: "BulkRejectRedemptionActions",
				APIPath: "/investment/redemption/initiation/reject", SubModule: mfSubRedemption, EntityCode: id, Actor: checkerBy},
				buildRedemptionInitiationPolicyFields(row)); !ok {
				api.RespondWithError(w, http.StatusUnprocessableEntity, id+": "+pmsg)
				return
			}
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactionredemption
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
			WHERE action_id = ANY($4)
		`, api.SystemIfBlank(checkerBy), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), actionIDs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
			return
		}

		// ── Approval-matrix engine: reject engine-managed records
		for _, rid := range req.RedemptionIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{
				ModuleCode: "INVESTMENT_MF", RecordID: rid,
				UserID: req.UserID, UserEmail: checkerBy,
				Action: approvalengine.ActionRejected, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[MFRedemptionInitiation] ActOnPendingOrDiagnose reject failed for %s: %v", rid, actionErr)
			}
			if actionRes.Acted {
				api.LogInfo("[MFRedemptionInitiation] engine rejected %s", rid)
			}
		}

		// Release blocked units for rejected redemptions with entity scoping
		for _, rid := range req.RedemptionIDs {
			var folioID, dematID, schemeID, entityName sql.NullString
			var byUnits sql.NullFloat64
			var method string

			if err := tx.QueryRow(ctx, `
				SELECT ri.folio_id, ri.demat_id, ri.scheme_id, ri.by_units, COALESCE(ms.method, 'FIFO') AS method, ri.entity_name
				FROM investment.redemption_initiation ri
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = ri.scheme_id OR
					ms.internal_scheme_code = ri.scheme_id OR
					ms.amfi_scheme_code = ri.scheme_id
				)
				WHERE ri.redemption_id = $1
			`, rid).Scan(&folioID, &dematID, &schemeID, &byUnits, &method, &entityName); err != nil {
				continue
			}

			if !byUnits.Valid || byUnits.Float64 <= 0 {
				continue
			}

			unblockQuery := `
				WITH target_transactions AS (
					SELECT 
						ot.id,
						COALESCE(ot.blocked_units, 0) AS current_blocked,
						ot.transaction_date
					FROM investment.onboard_transaction ot
					LEFT JOIN investment.masterscheme ms ON (
						COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))
					)
					WHERE 
						LOWER(COALESCE(ot.transaction_type, '')) IN ('buy', 'purchase', 'subscription')
						AND (
							ms.scheme_id = $1 OR
														ms.internal_scheme_code = $1 OR
														ot.scheme_id = $1 OR
							ot.scheme_internal_code = $1
						)
						AND (
							($2::text IS NOT NULL AND ot.folio_id = $2) OR
							($3::text IS NOT NULL AND ot.demat_id = $3)
						)
						AND ($6::text IS NULL OR ot.entity_name = $6)
						AND COALESCE(ot.blocked_units, 0) > 0
					ORDER BY 
						CASE WHEN $4 = 'FIFO' THEN ot.transaction_date END ASC,
						CASE WHEN $4 = 'LIFO' THEN ot.transaction_date END DESC
				),
				unblocking_allocation AS (
					SELECT 
						id,
						current_blocked,
						LEAST(
							current_blocked,
							$5 - COALESCE(SUM(LEAST(current_blocked, $5)) OVER (
								ORDER BY transaction_date
								ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
							), 0)
						) AS units_to_unblock_here
					FROM target_transactions
				)
				UPDATE investment.onboard_transaction ot
				SET blocked_units = GREATEST(0, COALESCE(ot.blocked_units, 0) - ua.units_to_unblock_here)
				FROM unblocking_allocation ua
				WHERE ot.id = ua.id AND ua.units_to_unblock_here > 0
			`

			var folioIDStr, dematIDStr, entityNameStr *string
			if folioID.Valid {
				folioIDStr = &folioID.String
			}
			if dematID.Valid {
				dematIDStr = &dematID.String
			}
			if entityName.Valid {
				entityNameStr = &entityName.String
			}

			if _, err := tx.Exec(ctx, unblockQuery,
				schemeID.String,
				nullIfEmptyStringPtr(folioIDStr),
				nullIfEmptyStringPtr(dematIDStr),
				method,
				byUnits.Float64,
				nullIfEmptyStringPtr(entityNameStr),
			); err != nil {
				// Log error but continue
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		if len(req.RedemptionIDs) > 0 {
			go func() {
				payload := BuildRedemptionInitiationNotifPayload(ctx, pgxPool, req.RedemptionIDs, constants.AuditActionReject, checkerBy)
				catalog.TriggerNotification(ctx, pgxPool, "/investment/redemption/initiation/reject", req.RedemptionIDs[0], payload.ToMap())
				for _, id := range req.RedemptionIDs {
					dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_MF", "MF_REDEMPTION", "POST_REJECT", []string{id}, checkerBy)
				}
			}()
		}
		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

// ---------------------------
// GetRedemptionsWithAudit
// ---------------------------

func GetRedemptionsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		out, err := fetchRedemptionInitiationRows(ctx, pgxPool, nil)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// fetchRedemptionInitiationRows is the single source-of-truth query for redemption initiations.
// ids=nil → all non-deleted rows (GET behaviour).
// ids!=nil → WHERE m.redemption_id = ANY($1) (payload builder filter).
func fetchRedemptionInitiationRows(ctx context.Context, pgxPool *pgxpool.Pool, ids []string) ([]map[string]interface{}, error) {
	const baseSQL = `
		WITH latest_audit AS (
			SELECT DISTINCT ON (a.redemption_id)
				a.redemption_id,
				a.actiontype,
				a.processing_status,
				a.action_id,
				a.requested_by,
				a.requested_at,
				a.checker_by,
				a.checker_at,
				a.checker_comment,
				a.reason
			FROM investment.auditactionredemption a
			WHERE UPPER(COALESCE(a.actiontype, '')) <> 'UPLOAD_FILE'
			ORDER BY a.redemption_id, a.requested_at DESC
		),
		history AS (
			SELECT 
				redemption_id,
				MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
				MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
				MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
				MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
				MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
				MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
			FROM investment.auditactionredemption
			GROUP BY redemption_id
		),
		resolved_folio AS (
			SELECT DISTINCT ON (m.redemption_id)
				m.redemption_id,
				f.folio_number,
				f.folio_id::text AS folio_id_text
			FROM investment.redemption_initiation m
			LEFT JOIN investment.masterfolio f ON (
				(f.folio_id::text = m.folio_id) OR 
				(m.folio_id IS NOT NULL AND f.folio_number = m.folio_id)
			)
			ORDER BY m.redemption_id, f.folio_id
		),
		resolved_demat AS (
			SELECT DISTINCT ON (m.redemption_id)
				m.redemption_id,
				d.demat_account_number,
				d.demat_id::text AS demat_id_text
			FROM investment.redemption_initiation m
			LEFT JOIN investment.masterdemataccount d ON (
				(d.demat_id::text = m.demat_id) OR 
				(m.demat_id IS NOT NULL AND d.default_settlement_account = m.demat_id) OR 
				(m.demat_id IS NOT NULL AND d.demat_account_number = m.demat_id)
			)
			ORDER BY m.redemption_id, d.demat_id
		)
		SELECT
			m.redemption_id,
			m.folio_id,
			m.old_folio_id,
			m.demat_id,
			m.old_demat_id,
			m.scheme_id,
			m.old_scheme_id,
			m.requested_by,
			COALESCE(m.entity_name,'') AS entity_name,
			COALESCE(s.scheme_id::text, m.scheme_id::text) AS resolved_scheme_id,
			COALESCE(s.scheme_name, m.scheme_id) AS scheme_name,
			COALESCE(s.internal_scheme_code,'') AS scheme_code,
			COALESCE(s.isin,'') AS isin,
			COALESCE(s.amc_name,'') AS amc_name,
			COALESCE(rf.folio_number,'') AS folio_number,
			COALESCE(rf.folio_id_text,'') AS folio_id_text,
			COALESCE(rd.demat_account_number,'') AS demat_number,
			COALESCE(rd.demat_id_text,'') AS demat_id_text,
			m.old_requested_by,
			TO_CHAR(m.requested_date, 'YYYY-MM-DD') AS requested_date,
			TO_CHAR(m.old_requested_date, 'YYYY-MM-DD') AS old_requested_date,
			TO_CHAR(m.transaction_date, 'YYYY-MM-DD') AS transaction_date,
			TO_CHAR(m.old_transaction_date, 'YYYY-MM-DD') AS old_transaction_date,
			m.estimated_proceeds,
			m.old_estimated_proceeds,
			m.gain_loss,
			m.old_gain_loss,
			DATE_PART('day', now()::timestamp - COALESCE(m.transaction_date, m.requested_date)::timestamp)::int AS age_days,
			m.by_amount,
			m.old_by_amount,
			m.by_units,
			m.old_by_units,
			COALESCE(s.method, 'FIFO') AS method,
			COALESCE(s.method, 'FIFO') AS old_method,
			m.is_deleted,
			TO_CHAR(m.updated_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
			
			COALESCE(l.actiontype,'') AS action_type,
			COALESCE(l.processing_status,'') AS processing_status,
			COALESCE(l.action_id::text,'') AS action_id,
			COALESCE(l.requested_by,'') AS audit_requested_by,
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
			-- ── Approval-engine columns ────────────────────────────────────────
			COALESCE(ai.instance_id,'')        AS approval_instance_id,
			COALESCE(ai.status,'')             AS approval_engine_status,
			COALESCE(aie.instance_eye_id,'')   AS current_eye_id,
			COALESCE(aie.position::text,'')    AS current_eye_position,
			COALESCE(aie.approvals_required,0) AS approvals_required,
			COALESCE(aie.approvals_received,0) AS approvals_received,
			aie.sla_deadline                   AS sla_deadline,
			COALESCE(aie.is_escalated,false)   AS is_escalated
		FROM investment.redemption_initiation m
		LEFT JOIN latest_audit l ON l.redemption_id = m.redemption_id
		LEFT JOIN history h ON h.redemption_id = m.redemption_id
		LEFT JOIN investment.masterscheme s ON (
		    s.scheme_id::text = m.scheme_id
		
		 OR s.internal_scheme_code = m.scheme_id
		
		)
		LEFT JOIN resolved_folio rf ON rf.redemption_id = m.redemption_id
		LEFT JOIN resolved_demat rd ON rd.redemption_id = m.redemption_id
		LEFT JOIN LATERAL (
			SELECT ai.* FROM uam.approval_instance ai
			WHERE ai.record_id = m.redemption_id::text
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
		q = baseSQL + " WHERE m.redemption_id = ANY($1) ORDER BY m.entity_name, m.redemption_id"
		args = []interface{}{ids}
	} else {
		args = []interface{}{}
		pos := 1
		where := " WHERE COALESCE(m.is_deleted, false) = false"
		if entityNames := redemptionEntityNameRefs(ctx); len(entityNames) > 0 {
			where += fmt.Sprintf(" AND (COALESCE(m.entity_name,'') = '' OR m.entity_name = ANY($%d::text[]))", pos)
			args = append(args, entityNames)
			pos++
		}
		if schemeRefs := redemptionMFSchemeRefs(ctx); len(schemeRefs) > 0 {
			where += fmt.Sprintf(" AND m.scheme_id = ANY($%d::text[])", pos)
			args = append(args, schemeRefs)
			pos++
		}
		if folioRefs := redemptionMFFolioRefs(ctx); len(folioRefs) > 0 {
			where += fmt.Sprintf(" AND (COALESCE(m.folio_id,'') = '' OR m.folio_id = ANY($%d::text[]))", pos)
			args = append(args, folioRefs)
			pos++
		}
		if dematRefs := redemptionMFDematRefs(ctx); len(dematRefs) > 0 {
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
			rec[string(f.Name)] = vals[i]
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// ---------------------------
// GetApprovedRedemptions
// ---------------------------

func GetApprovedRedemptions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
		WITH latest AS (
			SELECT DISTINCT ON (redemption_id)
				redemption_id,
				processing_status,
				requested_at,
				checker_at
			FROM investment.auditactionredemption
			WHERE UPPER(COALESCE(actiontype, '')) <> 'UPLOAD_FILE'
			ORDER BY redemption_id, requested_at DESC
		),
		resolved_folio AS (
			SELECT DISTINCT ON (m.redemption_id)
				m.redemption_id,
				f.folio_number,
				f.folio_id::text AS folio_id_text
			FROM investment.redemption_initiation m
			LEFT JOIN investment.masterfolio f ON (
				(f.folio_id::text = m.folio_id) OR 
				(m.folio_id IS NOT NULL AND f.folio_number = m.folio_id)
			)
			ORDER BY m.redemption_id, f.folio_id
		),
		resolved_demat AS (
			SELECT DISTINCT ON (m.redemption_id)
				m.redemption_id,
				d.demat_account_number,
				d.demat_id::text AS demat_id_text
			FROM investment.redemption_initiation m
			LEFT JOIN investment.masterdemataccount d ON (
				(d.demat_id::text = m.demat_id) OR 
				(m.demat_id IS NOT NULL AND d.default_settlement_account = m.demat_id) OR 
				(m.demat_id IS NOT NULL AND d.demat_account_number = m.demat_id)
			)
			ORDER BY m.redemption_id, d.demat_id
		)
		SELECT
			m.redemption_id,
			m.folio_id,
			m.demat_id,
			m.scheme_id,
			COALESCE(m.entity_name,'') AS entity_name,
			COALESCE(s.scheme_id::text, m.scheme_id::text) AS resolved_scheme_id,
			COALESCE(s.scheme_name, m.scheme_id) AS scheme_name,
			COALESCE(s.internal_scheme_code,'') AS scheme_code,
			COALESCE(s.isin,'') AS isin,
			COALESCE(s.amc_name,'') AS amc_name,
			COALESCE(rf.folio_number,'') AS folio_number,
			COALESCE(rf.folio_id_text,'') AS folio_id_text,
			COALESCE(rd.demat_account_number,'') AS demat_number,
			COALESCE(rd.demat_id_text,'') AS demat_id_text,
			m.requested_by,
			TO_CHAR(m.requested_date, 'YYYY-MM-DD') AS requested_date,
			TO_CHAR(m.transaction_date, 'YYYY-MM-DD') AS transaction_date,
			m.by_amount,
			m.by_units,
			COALESCE(s.method, 'FIFO') AS method,
			m.estimated_proceeds,
			m.gain_loss,
			DATE_PART('day', now()::timestamp - COALESCE(m.transaction_date, m.requested_date)::timestamp)::int AS age_days,
			COALESCE(l.processing_status, '') AS processing_status
		FROM investment.redemption_initiation m
		JOIN latest l ON l.redemption_id = m.redemption_id
		LEFT JOIN investment.masterscheme s ON (
		    s.scheme_id::text = m.scheme_id
		
		 OR s.internal_scheme_code = m.scheme_id
		
		)
		LEFT JOIN resolved_folio rf ON rf.redemption_id = m.redemption_id
		LEFT JOIN resolved_demat rd ON rd.redemption_id = m.redemption_id
		WHERE 
			UPPER(l.processing_status) = 'APPROVED'
			AND COALESCE(m.is_deleted,false)=false
			AND m.redemption_id NOT IN (
				SELECT redemption_id FROM investment.redemption_confirmation
				WHERE COALESCE(is_deleted, false) = false
			)
	`
		args := []interface{}{}
		pos := 1
		if entityNames := redemptionEntityNameRefs(ctx); len(entityNames) > 0 {
			q += fmt.Sprintf(" AND (COALESCE(m.entity_name,'') = '' OR m.entity_name = ANY($%d::text[]))", pos)
			args = append(args, entityNames)
			pos++
		}
		if schemeRefs := redemptionMFSchemeRefs(ctx); len(schemeRefs) > 0 {
			q += fmt.Sprintf(" AND m.scheme_id = ANY($%d::text[])", pos)
			args = append(args, schemeRefs)
			pos++
		}
		if folioRefs := redemptionMFFolioRefs(ctx); len(folioRefs) > 0 {
			q += fmt.Sprintf(" AND (COALESCE(m.folio_id,'') = '' OR m.folio_id = ANY($%d::text[]))", pos)
			args = append(args, folioRefs)
			pos++
		}
		if dematRefs := redemptionMFDematRefs(ctx); len(dematRefs) > 0 {
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
		out := make([]map[string]interface{}, 0, 100)
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				rec[string(f.Name)] = vals[i]
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "rows error: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// ---------------------------
// GetRedemptionInitiationDetail
// Returns deep info for a single redemption_id: initiation + audit + holding + lots + confirmations.
// ---------------------------

func GetRedemptionInitiationDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req GetRedemptionDetailRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.RedemptionID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "redemption_id is required")
			return
		}

		ctx := r.Context()

		// Pull initiation + latest audit + resolved folio/demat/scheme in one go.
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.redemption_id)
					a.redemption_id,
					a.actiontype,
					a.processing_status,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactionredemption a
				WHERE UPPER(COALESCE(a.actiontype, '')) <> 'UPLOAD_FILE'
				ORDER BY a.redemption_id, a.requested_at DESC
			),
			resolved_folio AS (
				SELECT DISTINCT ON (m.redemption_id)
					m.redemption_id,
					f.folio_id::text AS folio_id,
					f.folio_number,
					f.entity_name AS folio_entity_name,
					COALESCE(f.default_subscription_account,'') AS default_subscription_account,
					COALESCE(f.default_redemption_account,'') AS default_redemption_account
				FROM investment.redemption_initiation m
				LEFT JOIN investment.masterfolio f ON (
					(f.folio_id::text = m.folio_id) OR
					(m.folio_id IS NOT NULL AND f.folio_number = m.folio_id)
				)
				WHERE m.redemption_id = $1
				ORDER BY m.redemption_id, f.folio_id
			),
			resolved_demat AS (
				SELECT DISTINCT ON (m.redemption_id)
					m.redemption_id,
					d.demat_id::text AS demat_id,
					d.demat_account_number,
					d.entity_name AS demat_entity_name,
					COALESCE(d.default_settlement_account,'') AS default_settlement_account
				FROM investment.redemption_initiation m
				LEFT JOIN investment.masterdemataccount d ON (
					(d.demat_id::text = m.demat_id) OR
					(m.demat_id IS NOT NULL AND d.default_settlement_account = m.demat_id) OR
					(m.demat_id IS NOT NULL AND d.demat_account_number = m.demat_id)
				)
				WHERE m.redemption_id = $1
				ORDER BY m.redemption_id, d.demat_id
			)
			SELECT
				m.redemption_id,
				COALESCE(m.entity_name,'') AS entity_name,
				m.folio_id,
				m.demat_id,
				COALESCE(m.scheme_id,'') AS scheme_id,
				COALESCE(m.requested_by,'') AS requested_by,
				TO_CHAR(m.requested_date, 'YYYY-MM-DD') AS requested_date,
				COALESCE(TO_CHAR(m.transaction_date, 'YYYY-MM-DD'), '') AS transaction_date,
				COALESCE(m.by_amount, 0) AS by_amount,
				COALESCE(m.by_units, 0) AS by_units,
				COALESCE(s.method, 'FIFO') AS method,
				COALESCE(m.estimated_proceeds, 0) AS estimated_proceeds,
				COALESCE(m.gain_loss, 0) AS gain_loss,
				COALESCE(m.is_deleted,false) AS is_deleted,
				COALESCE(s.scheme_id::text, m.scheme_id::text) AS resolved_scheme_id,
				COALESCE(s.scheme_name, m.scheme_id) AS scheme_name,
				COALESCE(s.internal_scheme_code,'') AS scheme_code,
				COALESCE(s.isin,'') AS isin,
				COALESCE(s.amc_name,'') AS amc_name,
				COALESCE(rf.folio_id,'') AS resolved_folio_id,
				COALESCE(rf.folio_number,'') AS folio_number,
				COALESCE(rf.folio_entity_name,'') AS folio_entity_name,
				COALESCE(rd.demat_id,'') AS resolved_demat_id,
				COALESCE(rd.demat_account_number,'') AS demat_account_number,
				COALESCE(rd.demat_entity_name,'') AS demat_entity_name,
				COALESCE(rf.default_subscription_account,'') AS default_subscription_account,
				COALESCE(rf.default_redemption_account,'') AS default_redemption_account,
				COALESCE(rd.default_settlement_account,'') AS default_settlement_account,
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS audit_requested_by,
				COALESCE(TO_CHAR((l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS audit_requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				COALESCE(TO_CHAR((l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason
			FROM investment.redemption_initiation m
			LEFT JOIN latest_audit l ON l.redemption_id = m.redemption_id
			LEFT JOIN investment.masterscheme s ON (
				s.scheme_id::text = m.scheme_id
			
			 OR s.internal_scheme_code = m.scheme_id
			
			)
			LEFT JOIN resolved_folio rf ON rf.redemption_id = m.redemption_id
			LEFT JOIN resolved_demat rd ON rd.redemption_id = m.redemption_id
			WHERE m.redemption_id = $1
			LIMIT 1;
		`

		var (
			redemptionID         string
			entityName           string
			folioIDRaw           sql.NullString
			dematIDRaw           sql.NullString
			schemeIDRaw          string
			requestedBy          string
			requestedDate        string
			transactionDate      string
			byAmount             float64
			byUnits              float64
			method               string
			estimatedProceeds    float64
			gainLoss             float64
			isDeleted            bool
			resolvedSchemeID     string
			schemeName           string
			schemeCode           string
			isin                 string
			amcName              string
			resolvedFolioID      string
			folioNumber          string
			folioEntityName      string
			resolvedDematID      string
			dematAccountNumber   string
			dematEntityName      string
			defaultSubAcctRaw    string
			defaultRedAcctRaw    string
			defaultSettleAcctRaw string
			actionType           string
			processingStatus     string
			actionID             string
			auditRequestedBy     string
			auditRequestedAt     string
			checkerBy            string
			checkerAt            string
			checkerComment       string
			reason               string
		)

		err := pgxPool.QueryRow(ctx, q, req.RedemptionID).Scan(
			&redemptionID,
			&entityName,
			&folioIDRaw,
			&dematIDRaw,
			&schemeIDRaw,
			&requestedBy,
			&requestedDate,
			&transactionDate,
			&byAmount,
			&byUnits,
			&method,
			&estimatedProceeds,
			&gainLoss,
			&isDeleted,
			&resolvedSchemeID,
			&schemeName,
			&schemeCode,
			&isin,
			&amcName,
			&resolvedFolioID,
			&folioNumber,
			&folioEntityName,
			&resolvedDematID,
			&dematAccountNumber,
			&dematEntityName,
			&defaultSubAcctRaw,
			&defaultRedAcctRaw,
			&defaultSettleAcctRaw,
			&actionType,
			&processingStatus,
			&actionID,
			&auditRequestedBy,
			&auditRequestedAt,
			&checkerBy,
			&checkerAt,
			&checkerComment,
			&reason,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "redemption_id not found")
			} else {
				log.Printf("[ERROR] GetRedemptionInitiationDetail scan: redemption_id=%s err=%v", req.RedemptionID, err)
				api.RespondWithError(w, http.StatusInternalServerError, "failed to load redemption detail")
			}
			return
		}
		if isDeleted {
			api.RespondWithError(w, http.StatusNotFound, "redemption_id is deleted")
			return
		}

		// Resolve entity if missing on initiation.
		resolvedEntity := strings.TrimSpace(entityName)
		if resolvedEntity == "" {
			if strings.TrimSpace(folioEntityName) != "" {
				resolvedEntity = folioEntityName
			} else if strings.TrimSpace(dematEntityName) != "" {
				resolvedEntity = dematEntityName
			}
		}

		// Ensure we have an entity *name* for downstream SQL and account resolution.
		entityNameScoped := strings.TrimSpace(resolvedEntity)
		scope := ctxutil.FromContext(ctx)
		approvedNames := scope.EntityNames
		approvedIDs := scope.EntityIDs
		idToName := map[string]string{}
		minLen := len(approvedIDs)
		if len(approvedNames) < minLen {
			minLen = len(approvedNames)
		}
		for i := 0; i < minLen; i++ {
			idToName[strings.ToUpper(strings.TrimSpace(approvedIDs[i]))] = strings.TrimSpace(approvedNames[i])
		}
		if mapped, ok := idToName[strings.ToUpper(strings.TrimSpace(entityNameScoped))]; ok && strings.TrimSpace(mapped) != "" {
			entityNameScoped = strings.TrimSpace(mapped)
		}
		if entityNameScoped == "" {
			entityNameScoped = strings.TrimSpace(req.EntityName)
		}
		if mapped, ok := idToName[strings.ToUpper(strings.TrimSpace(entityNameScoped))]; ok && strings.TrimSpace(mapped) != "" {
			entityNameScoped = strings.TrimSpace(mapped)
		}
		// Final fallback: look up entity_name by entity_id if needed.
		if entityNameScoped != "" {
			if _, ok := idToName[strings.ToUpper(strings.TrimSpace(entityNameScoped))]; ok {
				// already mapped
			} else {
				var lookedUpName string
				// try possible tables in order until we find a non-empty entity_name
				tables := []string{"masterentitycash", "masterentity"}
				for _, tbl := range tables {
					query := fmt.Sprintf("SELECT entity_name FROM %s WHERE entity_id = $1 LIMIT 1", tbl)
					if err := pgxPool.QueryRow(ctx, query, entityNameScoped).Scan(&lookedUpName); err == nil {
						if strings.TrimSpace(lookedUpName) != "" {
							entityNameScoped = strings.TrimSpace(lookedUpName)
							break
						}
					}
				}
			}
		}

		// Scope check (critical): must be within entities granted by middleware.
		// NOTE: middleware provides BOTH entity names and entity IDs; some tables may store entity_id in `entity_name`.
		allowed := false
		if strings.TrimSpace(entityNameScoped) != "" && (scope.HasEntityNameAccess(entityNameScoped) || scope.HasEntityAccess(entityNameScoped)) {
			allowed = true
		} else if strings.TrimSpace(resolvedEntity) != "" && (scope.HasEntityNameAccess(resolvedEntity) || scope.HasEntityAccess(resolvedEntity)) {
			allowed = true
		} else if strings.TrimSpace(req.EntityName) != "" && (scope.HasEntityNameAccess(req.EntityName) || scope.HasEntityAccess(req.EntityName)) {
			allowed = true
		}
		if !allowed {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}

		// Re-resolve folio/demat under the scoped entity to avoid collisions (e.g., folio_number reused).
		folioIdentifier := strings.TrimSpace(folioIDRaw.String)
		if folioIdentifier == "" {
			folioIdentifier = strings.TrimSpace(folioNumber)
		}
		if folioIdentifier != "" {
			var fID, fNum, fRed string
			if err := pgxPool.QueryRow(ctx, `
				SELECT folio_id::text, folio_number, COALESCE(default_redemption_account,'')
				FROM investment.masterfolio
				WHERE COALESCE(is_deleted,false)=false
					AND LOWER(TRIM(entity_name)) = LOWER(TRIM($1))
					AND (folio_id::text = $2 OR folio_number = $2 OR folio_number = $3)
				LIMIT 1
			`, entityNameScoped, folioIdentifier, strings.TrimSpace(folioNumber)).Scan(&fID, &fNum, &fRed); err == nil {
				if strings.TrimSpace(fID) != "" {
					resolvedFolioID = strings.TrimSpace(fID)
				}
				if strings.TrimSpace(fNum) != "" {
					folioNumber = strings.TrimSpace(fNum)
				}
				if strings.TrimSpace(fRed) != "" {
					defaultRedAcctRaw = strings.TrimSpace(fRed)
				}
			}
		}

		dematIdentifier := strings.TrimSpace(dematIDRaw.String)
		if dematIdentifier == "" {
			dematIdentifier = strings.TrimSpace(dematAccountNumber)
		}
		if dematIdentifier != "" {
			var dID, dNum, dSettle string
			if err := pgxPool.QueryRow(ctx, `
				SELECT demat_id::text, demat_account_number, COALESCE(default_settlement_account,'')
				FROM investment.masterdemataccount
				WHERE COALESCE(is_deleted,false)=false
					AND LOWER(TRIM(entity_name)) = LOWER(TRIM($1))
					AND (demat_id::text = $2 OR demat_account_number = $2)
				LIMIT 1
			`, entityNameScoped, dematIdentifier).Scan(&dID, &dNum, &dSettle); err == nil {
				if strings.TrimSpace(dID) != "" {
					resolvedDematID = strings.TrimSpace(dID)
				}
				if strings.TrimSpace(dNum) != "" {
					dematAccountNumber = strings.TrimSpace(dNum)
				}
				if strings.TrimSpace(dSettle) != "" {
					defaultSettleAcctRaw = strings.TrimSpace(dSettle)
				}
			}
		}

		// Resolve default accounts to *account numbers* (never ids)
		defaultRedAcct := resolveMasterBankAccountNumber(ctx, pgxPool, entityNameScoped, defaultRedAcctRaw)
		defaultSettleAcct := resolveMasterBankAccountNumber(ctx, pgxPool, entityNameScoped, defaultSettleAcctRaw)
		creditBankAccount := ""
		if strings.TrimSpace(folioNumber) != "" {
			creditBankAccount = defaultRedAcct
		} else if strings.TrimSpace(dematAccountNumber) != "" {
			creditBankAccount = defaultSettleAcct
		} else if strings.TrimSpace(defaultRedAcct) != "" {
			creditBankAccount = defaultRedAcct
		} else {
			creditBankAccount = defaultSettleAcct
		}

		// Holding snapshot (best-effort)
		var (
			snapTotalUnits    float64
			snapAvgNav        float64
			snapCurrentNav    float64
			snapCurrentValue  float64
			snapTotalInvested float64
			snapGainLoss      float64
			snapGainLossPct   float64
		)
		folioID := sql.NullString{String: resolvedFolioID, Valid: strings.TrimSpace(resolvedFolioID) != ""}
		dematID := sql.NullString{String: resolvedDematID, Valid: strings.TrimSpace(resolvedDematID) != ""}
		folioNumArg := nullIfEmptyString(folioNumber)
		dematNumArg := nullIfEmptyString(dematAccountNumber)
		_ = pgxPool.QueryRow(ctx, `
			SELECT
				COALESCE(total_units,0),
				COALESCE(avg_nav,0),
				COALESCE(current_nav,0),
				COALESCE(current_value,0),
				COALESCE(total_invested_amount,0),
				COALESCE(gain_loss,0),
				COALESCE(gain_losss_percent,0)
			FROM investment.portfolio_snapshot
			WHERE entity_name=$1 AND scheme_id=$2
				AND ((folio_id IS NOT NULL AND folio_id=$3) OR (demat_id IS NOT NULL AND demat_id=$4)
					OR (folio_number IS NOT NULL AND folio_number=$5) OR (demat_acc_number IS NOT NULL AND demat_acc_number=$6))
			ORDER BY created_at DESC
			LIMIT 1
		`, entityNameScoped, resolvedSchemeID, folioID, dematID, folioNumArg, dematNumArg).Scan(
			&snapTotalUnits, &snapAvgNav, &snapCurrentNav, &snapCurrentValue, &snapTotalInvested, &snapGainLoss, &snapGainLossPct,
		)

		// Blocked units: freeze units for all open (pending/approved) redemption initiations that do NOT yet have a CONFIRMED confirmation.
		// Once a confirmation is CONFIRMED, the SELL transaction is created and units are no longer blocked.
		var blockedByRedemptions float64
		_ = pgxPool.QueryRow(ctx, `
			WITH latest_initiation_audit AS (
				SELECT DISTINCT ON (redemption_id)
					redemption_id,
					processing_status,
					requested_at
				FROM investment.auditactionredemption
				WHERE UPPER(COALESCE(actiontype, '')) <> 'UPLOAD_FILE'
				ORDER BY redemption_id, requested_at DESC
			),
			confirmed_redemptions AS (
				-- Redemptions that have a CONFIRMED confirmation (SELL already created)
				SELECT DISTINCT rc.redemption_id
				FROM investment.redemption_confirmation rc
				WHERE UPPER(COALESCE(rc.status,'')) = 'CONFIRMED'
			)
			SELECT COALESCE(SUM(COALESCE(ri.by_units,0)),0)
			FROM investment.redemption_initiation ri
			JOIN latest_initiation_audit l ON l.redemption_id = ri.redemption_id
			WHERE COALESCE(ri.is_deleted,false)=false
				AND UPPER(COALESCE(l.processing_status,'')) IN ('PENDING_APPROVAL','APPROVED')
				AND ri.redemption_id NOT IN (SELECT redemption_id FROM confirmed_redemptions)
				AND LOWER(TRIM(COALESCE(ri.entity_name,''))) = LOWER(TRIM($1))
				AND ri.scheme_id = $2
				AND (
					($3::text IS NOT NULL AND (ri.folio_id = $3 OR ri.folio_id = $5))
					OR ($4::text IS NOT NULL AND (ri.demat_id = $4 OR ri.demat_id = $6))
				)
		`, entityNameScoped, resolvedSchemeID,
			nullIfEmptyString(resolvedFolioID), nullIfEmptyString(resolvedDematID),
			nullIfEmptyString(folioNumber), nullIfEmptyString(dematAccountNumber)).Scan(&blockedByRedemptions)

		// Buy/Sell transaction breakdown for "from where" visibility.
		buyLots := make([]map[string]any, 0, 200)
		sellTxs := make([]map[string]any, 0, 100)
		var totalBlockedUnits float64
		var totalSellUnits float64
		var totalBuyUnits float64
		var totalBuyAmount float64
		var totalBuyNavUnits float64

		buyQ := `
			SELECT
				ot.id,
				ot.batch_id,
				TO_CHAR(ot.transaction_date, 'YYYY-MM-DD') AS transaction_date,
				ot.transaction_type,
				COALESCE(ot.amount,0) AS amount,
				COALESCE(ot.units,0) AS units,
				COALESCE(ot.nav,0) AS nav,
				COALESCE(ot.blocked_units,0) AS blocked_units,
				COALESCE(ot.folio_number,'') AS folio_number,
				COALESCE(ot.demat_acc_number,'') AS demat_acc_number
			FROM investment.onboard_transaction ot
			WHERE COALESCE(ot.entity_name,'') = $1
				AND LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription')
				AND (( $2::text IS NOT NULL AND ot.folio_number = $2) OR ($3::text IS NOT NULL AND ot.demat_acc_number = $3))
				AND ( ot.scheme_id = $4 OR ot.scheme_internal_code = $4 )
			ORDER BY
				CASE WHEN $5 = 'FIFO' THEN ot.transaction_date END ASC,
				CASE WHEN $5 = 'LIFO' THEN ot.transaction_date END DESC,
				ot.id ASC
		`
		rows, err := pgxPool.Query(ctx, buyQ, entityNameScoped, nullIfEmptyString(folioNumber), nullIfEmptyString(dematAccountNumber), resolvedSchemeID, method)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var (
					id      int64
					batchID string
					txDate  string
					txType  string
					amount  float64
					units   float64
					nav     float64
					blocked float64
					fn      string
					dn      string
				)
				if err := rows.Scan(&id, &batchID, &txDate, &txType, &amount, &units, &nav, &blocked, &fn, &dn); err != nil {
					continue
				}
				totalBuyUnits += units
				totalBuyAmount += amount
				totalBuyNavUnits += (nav * units)
				// We'll allocate blocked units across lots (FIFO) after reading all lots.
				_ = blocked
				avail := units
				buyLots = append(buyLots, map[string]any{
					"id":               id,
					"batch_id":         batchID,
					"transaction_date": txDate,
					"transaction_type": txType,
					"folio_number":     fn,
					"demat_acc_number": dn,
					"amount":           amount,
					"units":            units,
					"nav":              nav,
					"blocked_units":    0.0,
					"available_units":  avail,
				})
			}
		}

		// Allocate blocked units across buy lots based on method (FIFO/LIFO).
		blockedRemaining := blockedByRedemptions
		for i := range buyLots {
			if blockedRemaining <= 0 {
				break
			}
			u, _ := buyLots[i]["units"].(float64)
			if u <= 0 {
				continue
			}
			b := blockedRemaining
			if b > u {
				b = u
			}
			buyLots[i]["blocked_units"] = b
			buyLots[i]["available_units"] = u - b
			totalBlockedUnits += b
			blockedRemaining -= b
		}

		sellQ := `
			SELECT
				ot.id,
				ot.batch_id,
				TO_CHAR(ot.transaction_date, 'YYYY-MM-DD') AS transaction_date,
				ot.transaction_type,
				COALESCE(ot.amount,0) AS amount,
				COALESCE(ot.units,0) AS units,
				COALESCE(ot.nav,0) AS nav,
				COALESCE(ot.scheme_id,'') AS scheme_id,
				COALESCE(ot.scheme_internal_code,'') AS scheme_internal_code,
				COALESCE(ot.folio_number,'') AS folio_number,
				COALESCE(ot.folio_id,'') AS folio_id,
				COALESCE(ot.demat_acc_number,'') AS demat_acc_number,
				COALESCE(ot.demat_id,'') AS demat_id,
				COALESCE(ot.entity_name,'') AS entity_name,
				TO_CHAR(ot.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
			FROM investment.onboard_transaction ot
			LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id))))
			WHERE COALESCE(ot.entity_name,'') = $1
				AND LOWER(COALESCE(ot.transaction_type,'')) IN ('sell','redemption')
				AND (( $2::text IS NOT NULL AND ot.folio_number = $2) OR ($3::text IS NOT NULL AND ot.demat_acc_number = $3))
				AND ( ot.scheme_id = $4 OR ot.scheme_internal_code = $5 OR ms.scheme_id::text = $4 OR ms.internal_scheme_code = $5 OR ms.amfi_scheme_code = $6 )
			ORDER BY ot.transaction_date ASC, ot.id ASC
		`
		rows2, err := pgxPool.Query(ctx, sellQ, entityNameScoped, nullIfEmptyString(folioNumber), nullIfEmptyString(dematAccountNumber), resolvedSchemeID, resolvedSchemeID, isin, schemeName)
		if err == nil {
			defer rows2.Close()
			for rows2.Next() {
				var (
					id                 int64
					batchID            string
					txDate             string
					txType             string
					amount             float64
					units              float64
					nav                float64
					schemeIDVal        string
					schemeInternalCode string
					folioNumVal        string
					folioIDVal         string
					dematNumVal        string
					dematIDVal         string
					entityNameVal      string
					createdAt          string
				)
				if err := rows2.Scan(&id, &batchID, &txDate, &txType, &amount, &units, &nav,
					&schemeIDVal, &schemeInternalCode, &folioNumVal, &folioIDVal,
					&dematNumVal, &dematIDVal, &entityNameVal, &createdAt); err != nil {
					continue
				}
				totalSellUnits += math.Abs(units)
				sellTxs = append(sellTxs, map[string]any{
					"id":                   id,
					"batch_id":             batchID,
					"transaction_date":     txDate,
					"transaction_type":     txType,
					"amount":               amount,
					"units":                units,
					"nav":                  nav,
					"scheme_id":            schemeIDVal,
					"scheme_internal_code": schemeInternalCode,
					"folio_number":         folioNumVal,
					"folio_id":             folioIDVal,
					"demat_acc_number":     dematNumVal,
					"demat_id":             dematIDVal,
					"entity_name":          entityNameVal,
					"created_at":           createdAt,
				})
			}
		}

		// Confirmation progress - fetch all confirmations for this redemption with full details
		confirmations := make([]map[string]any, 0, 50)
		var confirmedUnits float64
		var confirmedAmount float64
		confQ := `
			SELECT 
				rc.redemption_confirm_id, 
				COALESCE(rc.status,''), 
				COALESCE(rc.actual_units,0), 
				COALESCE(rc.actual_nav,0), 
				COALESCE(rc.gross_proceeds,0), 
				COALESCE(rc.exit_load,0),
				COALESCE(rc.tds,0),
				COALESCE(rc.net_credited,0),
				COALESCE(rc.confirmed_by,''),
				COALESCE(TO_CHAR(rc.confirmed_at, 'YYYY-MM-DD HH24:MI:SS'),''),
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'') AS created_at
			FROM investment.redemption_confirmation rc
			LEFT JOIN (
				SELECT DISTINCT ON (redemption_confirm_id) 
					redemption_confirm_id, requested_at
				FROM investment.auditactionredemptionconfirmation
				ORDER BY redemption_confirm_id, requested_at ASC
			) a ON a.redemption_confirm_id = rc.redemption_confirm_id
			WHERE rc.redemption_id = $1
			ORDER BY a.requested_at DESC
		`
		fmt.Printf("[DEBUG] Querying confirmations for redemption_id=%s\n", redemptionID)
		cRows, err := pgxPool.Query(ctx, confQ, redemptionID)
		if err != nil {
			fmt.Printf("[DEBUG] Confirmations query error: %v\n", err)
		} else {
			defer cRows.Close()
			for cRows.Next() {
				var rcID, st, confirmedBy, confirmedAt, createdAt string
				var au, anav, gp, exitLoad, tds, nc float64
				if err := cRows.Scan(&rcID, &st, &au, &anav, &gp, &exitLoad, &tds, &nc, &confirmedBy, &confirmedAt, &createdAt); err != nil {
					fmt.Printf("[DEBUG] Confirmation scan error: %v\n", err)
					continue
				}
				fmt.Printf("[DEBUG] Found confirmation: %s, status=%s, units=%f\n", rcID, st, au)
				// Compute amount = nav * units
				computedAmount := anav * au
				// Only count units from CONFIRMED confirmations
				if strings.ToUpper(st) == "CONFIRMED" {
					confirmedUnits += au
					confirmedAmount += computedAmount
				}
				confirmations = append(confirmations, map[string]any{
					"redemption_confirm_id": rcID,
					"status":                st,
					"actual_units":          au,
					"actual_nav":            anav,
					"amount":                computedAmount, // nav * units
					"gross_proceeds":        gp,
					"exit_load":             exitLoad,
					"tds":                   tds,
					"net_credited":          nc,
					"confirmed_by":          confirmedBy,
					"confirmed_at":          confirmedAt,
					"created_at":            createdAt,
				})
			}
			fmt.Printf("[DEBUG] Total confirmations found: %d, confirmedUnits=%f\n", len(confirmations), confirmedUnits)
		}

		// Compose holding numbers from portfolio engine (live NAV + cost basis), with lot-level blocked units.
		portfolioRows, _ := portfolio.QueryEntityHoldings(ctx, pgxPool, entityNameScoped)
		matched := portfolio.MatchHoldingRow(portfolioRows, resolvedSchemeID, folioNumber, dematAccountNumber)

		holdingTotalUnits := totalBuyUnits - totalSellUnits
		if matched != nil {
			holdingTotalUnits = matched.TotalUnits
		}
		if holdingTotalUnits < 0 {
			holdingTotalUnits = 0
		}
		availableUnits := holdingTotalUnits - totalBlockedUnits
		if availableUnits < 0 {
			availableUnits = 0
		}
		holdingAvgNav := snapAvgNav
		holdingTotalInvested := snapTotalInvested
		holdingCurrentNav := snapCurrentNav
		holdingCurrentValue := snapCurrentValue
		holdingGainLoss := snapGainLoss
		holdingGainLossPct := snapGainLossPct
		if matched != nil {
			holdingAvgNav = matched.AvgNav
			holdingTotalInvested = matched.TotalInvestedAmount
			holdingCurrentNav = matched.CurrentNav
			holdingCurrentValue = matched.CurrentValue
			holdingGainLoss = matched.GainLoss
			holdingGainLossPct = matched.GainLossPercent
		} else {
			if totalBuyUnits > 0 {
				holdingAvgNav = totalBuyNavUnits / totalBuyUnits
			}
			if holdingCurrentNav == 0 {
				holdingCurrentNav = holdingAvgNav
			}
			if holdingCurrentNav > 0 {
				holdingCurrentValue = holdingTotalUnits * holdingCurrentNav
			}
			if totalBuyAmount > 0 && holdingTotalUnits > 0 && totalBuyUnits > 0 {
				holdingTotalInvested = holdingTotalUnits * (totalBuyNavUnits / totalBuyUnits)
			}
			holdingGainLoss = holdingCurrentValue - holdingTotalInvested
			if holdingTotalInvested != 0 {
				holdingGainLossPct = (holdingGainLoss / holdingTotalInvested) * 100.0
			}
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"approval_workflow": loadMFRedemptionApprovalWorkflow(ctx, pgxPool, redemptionID, api.GetUserIDFromCtx(ctx)),
			"redemption": map[string]any{
				"redemption_id":              redemptionID,
				"entity_name":                entityNameScoped,
				"folio_id":                   strings.TrimSpace(folioIDRaw.String),
				"demat_id":                   strings.TrimSpace(dematIDRaw.String),
				"scheme_id":                  schemeIDRaw,
				"resolved_scheme_id":         resolvedSchemeID,
				"scheme_name":                schemeName,
				"scheme_code":                schemeCode,
				"isin":                       isin,
				"amc_name":                   amcName,
				"resolved_folio_id":          resolvedFolioID,
				"folio_number":               folioNumber,
				"resolved_demat_id":          resolvedDematID,
				"demat_account_number":       dematAccountNumber,
				"requested_by":               requestedBy,
				"requested_date":             requestedDate,
				"transaction_date":           transactionDate,
				"by_amount":                  byAmount,
				"by_units":                   byUnits,
				"method":                     method,
				"estimated_proceeds":         estimatedProceeds,
				"gain_loss":                  gainLoss,
				"default_redemption_account": defaultRedAcct,
				"default_settlement_account": defaultSettleAcct,
				"credit_bank_account":        creditBankAccount,
				"audit": map[string]any{
					"action_type":       actionType,
					"processing_status": processingStatus,
					"action_id":         actionID,
					"requested_by":      auditRequestedBy,
					"requested_at":      auditRequestedAt,
					"checker_by":        checkerBy,
					"checker_at":        checkerAt,
					"checker_comment":   checkerComment,
					"reason":            reason,
				},
			},
			"holding": map[string]any{
				"entity_name":           entityNameScoped,
				"folio_number":          folioNumber,
				"demat_acc_number":      dematAccountNumber,
				"scheme_id":             resolvedSchemeID,
				"scheme_name":           schemeName,
				"isin":                  isin,
				"total_units":           holdingTotalUnits,
				"blocked_units":         totalBlockedUnits,
				"available_units":       availableUnits,
				"avg_nav":               holdingAvgNav,
				"current_nav":           holdingCurrentNav,
				"current_value":         holdingCurrentValue,
				"total_invested_amount": holdingTotalInvested,
				"gain_loss":             holdingGainLoss,
				"gain_loss_percent":     holdingGainLossPct,
				"realized_gain_loss": func() float64 {
					if matched != nil {
						return matched.RealizedGainLoss
					}
					return 0
				}(),
				"total_gain_loss": func() float64 {
					if matched != nil {
						return matched.TotalGainLoss
					}
					return holdingGainLoss
				}(),
			},
			"buy_lots":      buyLots,
			"sell_txs":      sellTxs,
			"confirmations": confirmations,
			"summary": map[string]any{
				"requested_units":         byUnits,
				"requested_amount":        byAmount,
				"confirmed_units":         confirmedUnits,
				"confirmed_amount":        confirmedAmount,                    // nav * units from confirmed confirmations
				"already_redeemed_units":  totalSellUnits,                     // from SELL transactions
				"already_redeemed_amount": totalSellUnits * holdingCurrentNav, // estimated using current nav
				"currently_blocked_units": totalBlockedUnits,
				"holding_total_units":     holdingTotalUnits,
				"holding_available_units": availableUnits,
			},
		})
	}
}

// ---------------------------
// Helper functions
// ---------------------------

func nullIfEmptyString(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func nullIfEmptyStringPtr(s *string) interface{} {
	if s == nil || strings.TrimSpace(*s) == "" {
		return nil
	}
	return *s
}

func nullIfZeroFloat(f float64) interface{} {
	if f == 0 {
		return nil
	}
	return f
}
