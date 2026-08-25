package bankstatement

import (
	apictx "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/ctxutil"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"
	"CimplrCorpSaas/internal/validation"
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

// bankStatementApproveRoute is the policy-engine API path recorded for bank
// statement approval enforcement checks.
const bankStatementApproveRoute = "/cash/bank-statements/v2/approve"

func auditActorDisplayName(ctx context.Context, userID string) string {
	if session := middlewares.GetSessionFromContext(ctx); session != nil {
		if name := strings.TrimSpace(session.Name); name != "" {
			return name
		}
		if email := strings.TrimSpace(session.Email); email != "" {
			return email
		}
		if id := strings.TrimSpace(session.UserID); id != "" {
			return id
		}
	}

	for _, session := range auth.GetActiveSessions() {
		if session.UserID != userID {
			continue
		}
		if name := strings.TrimSpace(session.Name); name != "" {
			return name
		}
		if email := strings.TrimSpace(session.Email); email != "" {
			return email
		}
		if id := strings.TrimSpace(session.UserID); id != "" {
			return id
		}
	}

	return strings.TrimSpace(userID)
}

func validateCashMasterScope(ctx context.Context, fields map[string]interface{}) string {
	return validation.ValidateCashMasterReferences(ctx, fields)
}

func scopeValues(rows []map[string]string, key string) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if value := strings.TrimSpace(row[key]); value != "" {
			out = append(out, value)
		}
	}
	return out
}

// 1. Get all bank statements (POST, req: user_id)
func GetAllBankStatementsHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			http.Error(w, "Missing or invalid user_id in body", http.StatusBadRequest)
			return
		}
		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		entityIDs := scope.EntityIDs
		if len(entityIDs) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusUnauthorized)
			return
		}
		accountNumbers := scopeValues(scope.BankAccounts, "account_number")
		bankIDs := scopeValues(scope.Banks, "bank_id")
		currencyCodes := scopeValues(scope.Currencies, "currency_code")
		rows, err := pool.Query(ctx, `
										WITH scoped_statements AS (
											SELECT s.bank_statement_id, s.entity_id, e.entity_name, s.account_number, s.statement_period_start, s.statement_period_end, s.opening_balance, s.closing_balance, s.uploaded_at,
														COALESCE(mb.bank_name, '') AS bank_name,
														mba.account_nickname AS account_nickname,
														s.upload_s3_key
											FROM cimplrcorpsaas.bank_statements s
											JOIN public.masterentitycash e ON s.entity_id = e.entity_id
											LEFT JOIN public.masterbankaccount mba ON mba.account_number = s.account_number AND mba.is_deleted = false
											LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
											WHERE s.entity_id = ANY($1)
											  AND (cardinality($2::text[]) = 0 OR s.account_number = ANY($2::text[]))
											  AND (cardinality($3::text[]) = 0 OR mba.bank_id = ANY($3::text[]))
											  AND (cardinality($4::text[]) = 0 OR mba.currency = ANY($4::text[]))
											  AND COALESCE(s.is_deleted, false) = false
										),
										ordered_audit AS (
											SELECT a.*,
												GREATEST(a.requested_at, COALESCE(a.checker_at, a.requested_at)) AS event_at,
												ROW_NUMBER() OVER (
													PARTITION BY a.bankstatementid
													ORDER BY GREATEST(a.requested_at, COALESCE(a.checker_at, a.requested_at)) DESC,
													         a.action_id DESC
												) AS head_rn
											FROM cimplrcorpsaas.auditactionbankstatement a
											JOIN scoped_statements ss ON a.bankstatementid = ss.bank_statement_id
											WHERE COALESCE(a.actiontype, '') NOT IN ('UPLOAD_FILE', 'DOWNLOAD', 'DMS_TRIGGER')
										),
										head_audit AS (
											SELECT * FROM ordered_audit WHERE head_rn = 1
										),
										-- Display status: skip RECAT (any) and EDIT+PENDING_EDIT_APPROVAL;
										-- DELETE/CREATE pending still win via CASE priority.
										display_ranked AS (
											SELECT a.*,
												ROW_NUMBER() OVER (
													PARTITION BY a.bankstatementid
													ORDER BY
														CASE
															WHEN a.actiontype = 'DELETE' AND a.processing_status = 'PENDING_DELETE_APPROVAL' THEN 0
															WHEN a.actiontype = 'CREATE' AND a.processing_status = 'PENDING_APPROVAL' THEN 1
															ELSE 2
														END,
														a.event_at DESC,
														a.action_id DESC
												) AS disp_rn
											FROM ordered_audit a
											WHERE NOT (
												a.actiontype = 'RECAT'
												OR (a.actiontype = 'EDIT' AND a.processing_status = 'PENDING_EDIT_APPROVAL')
											)
										),
										display_audit AS (
											SELECT * FROM display_ranked WHERE disp_rn = 1
										)
										SELECT ss.bank_statement_id, ss.entity_id, ss.entity_name, ss.account_number, ss.statement_period_start, ss.statement_period_end, ss.opening_balance, ss.closing_balance, ss.uploaded_at,
													 ha.actiontype,
													 da.processing_status,
													 da.action_id, da.requested_by, da.requested_at, da.checker_by, da.checker_at, da.checker_comment, da.reason,
														ss.bank_name,
														ss.account_nickname,
														ss.upload_s3_key,
														(ha.actiontype = 'RECAT' AND ha.processing_status = 'PENDING_EDIT_APPROVAL') AS has_pending_recat
										FROM scoped_statements ss
										LEFT JOIN head_audit ha ON ha.bankstatementid = ss.bank_statement_id
										LEFT JOIN display_audit da ON da.bankstatementid = ss.bank_statement_id
										ORDER BY GREATEST(COALESCE(ha.requested_at, ss.uploaded_at), COALESCE(ha.checker_at, ss.uploaded_at)) DESC
						`, entityIDs, accountNumbers, bankIDs, currencyCodes)
		if err != nil {
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		resp := []map[string]interface{}{}
		for rows.Next() {
			var id, entityID, entityName, acc string
			var start, end, uploaded time.Time
			var open, close float64
			var actionType, processingStatus, actionID, requestedBy, checkerBy, checkerComment, reason sql.NullString
			var bankName sql.NullString
			var accountNickname sql.NullString
			var uploadS3Key sql.NullString
			var requestedAt, checkerAt sql.NullTime
			var hasPendingRecat bool
			if err := rows.Scan(&id, &entityID, &entityName, &acc, &start, &end, &open, &close, &uploaded,
				&actionType, &processingStatus, &actionID, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason, &bankName, &accountNickname, &uploadS3Key, &hasPendingRecat); err != nil {
				continue
			}
			// Display status drives delete-pending flag (head action may still be RECAT).
			isDeletePending := processingStatus.String == constants.StatusPendingDeleteApproval
			resp = append(resp, map[string]interface{}{
				"statement_id":               id,
				"bank_statement_id":          id,
				"entity_id":                  entityID,
				"entity_name":                entityName,
				"account_number":             acc,
				"statement_period_start":     start,
				"statement_period_end":       end,
				"opening_balance":            open,
				"closing_balance":            close,
				"uploaded_at":                uploaded,
				"action_type":                actionType.String,
				"processing_status":          processingStatus.String,
				"action_id":                  actionID.String,
				"requested_by":               requestedBy.String,
				"requested_at":               requestedAt.Time,
				"checker_by":                 checkerBy.String,
				"checker_at":                 checkerAt.Time,
				"checker_comment":            checkerComment.String,
				"reason":                     reason.String,
				"bank_name":                  bankName.String,
				"account_nickname":           accountNickname.String,
				"upload_s3_key":              uploadS3Key.String,
				"is_delete_pending_approval": isDeletePending,
				"has_pending_recat":          hasPendingRecat,
			})
		}
		apictx.RespondEnvelopeSuccess(w, "Success", resp)
	})
}

// 2. Get all transactions for a bank statement (POST, req: user_id, bank_statement_id)
func GetBankStatementTransactionsHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			UserID          string `json:"user_id"`
			BankStatementID string `json:"bank_statement_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil ||
			body.UserID == "" || body.BankStatementID == "" {
			http.Error(w, constants.ErrMissingUserIDOrBankStatementID, http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		entityIDs := scope.EntityIDs
		if len(entityIDs) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusUnauthorized)
			return
		}
		accountNumbers := scopeValues(scope.BankAccounts, "account_number")
		bankIDs := scopeValues(scope.Banks, "bank_id")
		currencyCodes := scopeValues(scope.Currencies, "currency_code")
		rows, err := pool.Query(ctx, `
			SELECT
				t.transaction_id,
				COALESCE(s.entity_id, '') AS entity_id,
				e.entity_name,
				t.tran_id,
				t.value_date,
				t.transaction_date,
				t.description,
				t.withdrawal_amount,
				t.deposit_amount,
				-- Use stored balance when non-zero; otherwise compute running balance
				-- from opening_balance + cumulative (deposits - withdrawals) ordered
				-- by value_date. This fixes statements committed before the running-
				-- balance write was added to CommitHandler.
				CASE
					WHEN COALESCE(t.balance, 0) <> 0 THEN t.balance
					ELSE ROUND(
						COALESCE(s.opening_balance, 0)
						+ SUM(COALESCE(t.deposit_amount, 0) - COALESCE(t.withdrawal_amount, 0))
							OVER (
								PARTITION BY t.bank_statement_id
								ORDER BY t.value_date, t.transaction_id
								ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
							),
						2
					)
				END AS balance,
				c.category_name,
				t.category_id,
				COALESCE(t.misclassified_flag, false)            AS misclassified_flag,
				COALESCE(t.narration_clean, t.description, '')   AS narration_clean,
				COALESCE(t.narration_ref, '')                    AS narration_ref,
				COALESCE(t.payment_channel, '')                  AS payment_channel,
				t.confidence_score,
				COALESCE(t.classification_step, '')              AS classification_step,
				-- Basis/reason for the classification (latest audit log entry)
				COALESCE(cal.source_ref, '')                     AS classification_basis,
				COALESCE(t.ai_reasoning, '')                     AS ai_reasoning,
				-- Review queue fields (only the latest PENDING entry, if any)
				COALESCE(q.status, '')                           AS review_status,
				COALESCE(q.suggested_cat::text, '')              AS suggested_cat_id,
				COALESCE(sq.category_name, '')                   AS suggested_cat_name
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements s
				ON t.bank_statement_id = s.bank_statement_id
			JOIN public.masterentitycash e
				ON s.entity_id = e.entity_id
			LEFT JOIN public.masterbankaccount mba
				ON mba.account_number = s.account_number AND mba.is_deleted = false
			LEFT JOIN public.mastercashflowcategory c
				ON t.category_id = c.category_id
			LEFT JOIN LATERAL (
				SELECT source_ref
				FROM cimplrcorpsaas.classification_audit_log
				WHERE transaction_id = t.transaction_id
				ORDER BY classified_at DESC
				LIMIT 1
			) cal ON TRUE
			LEFT JOIN LATERAL (
				SELECT status, suggested_cat
				FROM cimplrcorpsaas.categorization_review_queue
				WHERE transaction_id = t.transaction_id
				  AND status = 'PENDING'
				ORDER BY queue_id DESC
				LIMIT 1
			) q ON TRUE
			LEFT JOIN public.mastercashflowcategory sq
				ON sq.category_id::text = q.suggested_cat::text
			WHERE t.bank_statement_id = $1
			  AND s.entity_id = ANY($2)
			  AND (cardinality($3::text[]) = 0 OR s.account_number = ANY($3::text[]))
			  AND (cardinality($4::text[]) = 0 OR mba.bank_id = ANY($4::text[]))
			  AND (cardinality($5::text[]) = 0 OR mba.currency = ANY($5::text[]))
			ORDER BY t.value_date, t.transaction_id
		`, body.BankStatementID, entityIDs, accountNumbers, bankIDs, currencyCodes)

		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		resp := []map[string]interface{}{}

		for rows.Next() {
			var (
				tid                 int64
				entityID            string
				entityName          string
				tranID              sql.NullString
				desc                string
				category            sql.NullString
				categoryID          sql.NullString
				vdate               time.Time
				tdate               time.Time
				withdrawal          sql.NullFloat64
				deposit             sql.NullFloat64
				balance             sql.NullFloat64
				misclassified       bool
				narrationClean      string
				narrationRef        string
				paymentChannel      string
				confidenceScore     sql.NullFloat64
				classificationStep  string
				classificationBasis string
				aiReasoning         string
				reviewStatus        string
				suggestedCatID      string
				suggestedCatName    string
			)

			if err := rows.Scan(
				&tid,
				&entityID,
				&entityName,
				&tranID,
				&vdate,
				&tdate,
				&desc,
				&withdrawal,
				&deposit,
				&balance,
				&category,
				&categoryID,
				&misclassified,
				&narrationClean,
				&narrationRef,
				&paymentChannel,
				&confidenceScore,
				&classificationStep,
				&classificationBasis,
				&aiReasoning,
				&reviewStatus,
				&suggestedCatID,
				&suggestedCatName,
			); err != nil {
				continue
			}

			categoryName := category.String
			if !category.Valid || categoryName == "" {
				categoryName = "Uncategorized"
			}

			var confScore *float64
			if confidenceScore.Valid {
				confScore = &confidenceScore.Float64
			}

			// Derive a single deterministic status for the UI.
			// Priority: manual override → auto-confirmed → pending analyst review → uncategorized
			var categorizationStatus string
			switch {
			case classificationStep == "CORRECTION" || classificationStep == "CONFIRMATION":
				categorizationStatus = "manual"
			case categoryID.Valid && categoryID.String != "":
				categorizationStatus = "auto"
			case reviewStatus == constants.StatusPending:
				categorizationStatus = "pending_review"
			default:
				categorizationStatus = "uncategorized"
			}

			reviewSuggested := reviewStatus == constants.StatusPending

			resp = append(resp, map[string]interface{}{
				"transaction_id":    tid,
				"entity_id":         entityID,
				"entity_name":       entityName,
				"tran_id":           tranID.String,
				"value_date":        vdate,
				"transaction_date":  tdate,
				"description":       desc,
				"withdrawal_amount": withdrawal.Float64,
				"deposit_amount":    deposit.Float64,
				"balance": func() interface{} {
					if balance.Valid {
						return balance.Float64
					}
					return nil
				}(),
				"category_name":      categoryName,
				"category_id":        categoryID.String,
				"misclassified_flag": misclassified,
				// Smart categorization fields
				"narration_clean":      narrationClean,
				"narration_ref":        narrationRef,
				"payment_channel":      paymentChannel,
				"confidence_score":     confScore,
				"classification_step":  classificationStep,
				"classification_basis": classificationBasis,
				"ai_reasoning":         aiReasoning,
				// Review / status fields
				"categorization_status": categorizationStatus,
				"review_suggested":      reviewSuggested,
				"suggested_cat_id":      suggestedCatID,
				"suggested_cat_name":    suggestedCatName,
			})
		}

		apictx.RespondEnvelopeSuccess(w, "Success", resp)
	})
}

func GetBankStatementDownloadURLHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			UserID          string `json:"user_id"`
			BankStatementID string `json:"bank_statement_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.BankStatementID) == "" {
			apictx.RespondEnvelopeError(w, http.StatusBadRequest, constants.ErrBankStatementIDRequired, "")
			return
		}

		ctx := r.Context()
		var uploadS3Key sql.NullString
		var entityName sql.NullString
		var auditFileID sql.NullString
		var entityID, accountNumber, bankID, currency string
		err := pool.QueryRow(ctx, `
			SELECT s.upload_s3_key, COALESCE(e.entity_name, '') AS entity_name, p.id,
			       COALESCE(s.entity_id, ''), COALESCE(s.account_number, ''),
			       COALESCE(mba.bank_id, ''), COALESCE(mba.currency, '')
			FROM cimplrcorpsaas.bank_statements s
			LEFT JOIN public.masterentitycash e ON s.entity_id = e.entity_id
			LEFT JOIN cimplrcorpsaas.bank_pdf_uploads p ON p.storage_path = s.upload_s3_key
			LEFT JOIN public.masterbankaccount mba ON mba.account_number = s.account_number AND mba.is_deleted = false
			WHERE s.bank_statement_id = $1
		`, body.BankStatementID).Scan(&uploadS3Key, &entityName, &auditFileID, &entityID, &accountNumber, &bankID, &currency)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
				apictx.RespondEnvelopeError(w, http.StatusNotFound, "bank_statement_id not found", "")
				return
			}
			apictx.RespondEnvelopeError(w, http.StatusInternalServerError, pqUserFriendlyMessage(err), "")
			return
		}
		if msg := validateCashMasterScope(ctx, map[string]interface{}{
			"entity_id":      entityID,
			"entity_name":    entityName.String,
			"account_number": accountNumber,
			"bank_id":        bankID,
			"currency":       currency,
		}); msg != "" {
			apictx.RespondEnvelopeError(w, http.StatusForbidden, msg, "")
			return
		}

		if !uploadS3Key.Valid || strings.TrimSpace(uploadS3Key.String) == "" {
			apictx.RespondEnvelopeError(w, http.StatusNotFound, "no file available", "")
			return
		}

		downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, uploadS3Key.String, 15*time.Minute)
		if err != nil {
			apictx.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to generate download URL", "")
			return
		}

		requestedBy := requestedByFromCtx(ctx, strings.TrimSpace(body.UserID))
		if requestedBy == "" {
			requestedBy = strings.TrimSpace(body.UserID)
		}
		if requestedBy != "" {
			auditFileID.String = strings.TrimSpace(auditFileID.String)
			auditFileID.Valid = auditFileID.String != ""
			if err := insertDownloadAuditPgx(
				ctx,
				pool,
				auditFileID,
				sql.NullString{String: strings.TrimSpace(body.BankStatementID), Valid: strings.TrimSpace(body.BankStatementID) != ""},
				requestedBy,
				apictx.ClientIPFromRequest(r),
				entityName,
			); err != nil {
				logger.LogError("failed to insert bank statement download audit for %s: %v", body.BankStatementID, err)
			}
		}

		apictx.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"download_url": downloadURL,
		})
	})
}

func GetBankStatementBulkDownloadURLHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			UserID           string   `json:"user_id"`
			BankStatementIDs []string `json:"bank_statement_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.BankStatementIDs) == 0 {
			apictx.RespondEnvelopeError(w, http.StatusBadRequest, "bank_statement_ids is required", "")
			return
		}

		ctx := r.Context()
		files := make([]map[string]string, 0, len(body.BankStatementIDs))
		failedIDs := make([]string, 0)

		for _, rawID := range body.BankStatementIDs {
			bankStatementID := strings.TrimSpace(rawID)
			if bankStatementID == "" {
				continue
			}

			var uploadS3Key sql.NullString
			var entityName sql.NullString
			var auditFileID sql.NullString
			var entityID, accountNumber, bankID, currency string
			err := pool.QueryRow(ctx, `
				SELECT s.upload_s3_key, COALESCE(e.entity_name, '') AS entity_name, p.id,
				       COALESCE(s.entity_id, ''), COALESCE(s.account_number, ''),
				       COALESCE(mba.bank_id, ''), COALESCE(mba.currency, '')
				FROM cimplrcorpsaas.bank_statements s
				LEFT JOIN public.masterentitycash e ON s.entity_id = e.entity_id
				LEFT JOIN cimplrcorpsaas.bank_pdf_uploads p ON p.storage_path = s.upload_s3_key
				LEFT JOIN public.masterbankaccount mba ON mba.account_number = s.account_number AND mba.is_deleted = false
				WHERE s.bank_statement_id = $1
			`, bankStatementID).Scan(&uploadS3Key, &entityName, &auditFileID, &entityID, &accountNumber, &bankID, &currency)
			if err != nil {
				failedIDs = append(failedIDs, bankStatementID)
				continue
			}
			if msg := validateCashMasterScope(ctx, map[string]interface{}{
				"entity_id":      entityID,
				"entity_name":    entityName.String,
				"account_number": accountNumber,
				"bank_id":        bankID,
				"currency":       currency,
			}); msg != "" {
				failedIDs = append(failedIDs, bankStatementID)
				continue
			}

			key := strings.TrimSpace(uploadS3Key.String)
			if !uploadS3Key.Valid || key == "" {
				failedIDs = append(failedIDs, bankStatementID)
				continue
			}

			downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, key, 15*time.Minute)
			if err != nil {
				failedIDs = append(failedIDs, bankStatementID)
				continue
			}

			requestedBy := requestedByFromCtx(ctx, strings.TrimSpace(body.UserID))
			if requestedBy == "" {
				requestedBy = strings.TrimSpace(body.UserID)
			}
			if requestedBy != "" {
				auditFileID.String = strings.TrimSpace(auditFileID.String)
				auditFileID.Valid = auditFileID.String != ""
				if err := insertDownloadAuditPgx(
					ctx,
					pool,
					auditFileID,
					sql.NullString{String: strings.TrimSpace(bankStatementID), Valid: strings.TrimSpace(bankStatementID) != ""},
					requestedBy,
					apictx.ClientIPFromRequest(r),
					entityName,
				); err != nil {
					logger.LogError("failed to insert bank statement bulk download audit for %s: %v", bankStatementID, err)
				}
			}

			files = append(files, map[string]string{
				"bank_statement_id": bankStatementID,
				"download_url":      downloadURL,
			})
		}

		if len(files) == 0 {
			apictx.RespondEnvelopeFailureWithData(w, http.StatusNotFound, "no downloadable files found", "", map[string]interface{}{
				"files":      []map[string]string{},
				"failed_ids": failedIDs,
			})
			return
		}

		apictx.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"files":      files,
			"failed_ids": failedIDs,
		})
	})
}

// 2a. Mark transactions as misclassified
func MarkBankStatementTransactionsMisclassifiedHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			UserID         string  `json:"user_id"`
			TransactionIDs []int64 `json:"transaction_ids"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || len(body.TransactionIDs) == 0 {
			http.Error(w, "Missing user_id or transaction_ids", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		scopeRows, err := pool.Query(ctx, `
			SELECT DISTINCT COALESCE(s.entity_id, ''), COALESCE(t.account_number, ''),
			       COALESCE(mba.bank_id, ''), COALESCE(mba.currency, '')
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements s ON s.bank_statement_id = t.bank_statement_id
			LEFT JOIN public.masterbankaccount mba ON mba.account_number = t.account_number AND mba.is_deleted = false
			WHERE t.transaction_id = ANY($1)
			  AND COALESCE(s.is_deleted, false) = false
		`, body.TransactionIDs)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer scopeRows.Close()
		for scopeRows.Next() {
			var entityID, accountNumber, bankID, currency string
			if err := scopeRows.Scan(&entityID, &accountNumber, &bankID, &currency); err != nil {
				http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
				return
			}
			if msg := validateCashMasterScope(ctx, map[string]interface{}{
				"entity_id":      entityID,
				"account_number": accountNumber,
				"bank_id":        bankID,
				"currency":       currency,
			}); msg != "" {
				http.Error(w, msg, http.StatusForbidden)
				return
			}
		}
		if err := scopeRows.Err(); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		res, err := pool.Exec(ctx, `
			UPDATE cimplrcorpsaas.bank_statement_transactions
			SET misclassified_flag = true
			WHERE transaction_id = ANY($1)
		`, body.TransactionIDs)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		rowsAffected := res.RowsAffected()

		apictx.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"updated_count": rowsAffected,
		})
	})
}

// 2b. Recompute KPIs and uncategorized data for an existing bank statement
func RecomputeBankStatementSummaryHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID          string `json:"user_id"`
			BankStatementID string `json:"bank_statement_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.BankStatementID == "" {
			http.Error(w, "Missing bank_statement_id in body", http.StatusBadRequest)
			return
		}

		ctx := r.Context()

		var entityID, accountNumber string
		var statementPeriodStart, statementPeriodEnd time.Time
		err := pool.QueryRow(ctx, `
				SELECT entity_id, account_number, statement_period_start, statement_period_end
				FROM cimplrcorpsaas.bank_statements
				WHERE bank_statement_id = $1
			`, body.BankStatementID).Scan(&entityID, &accountNumber, &statementPeriodStart, &statementPeriodEnd)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
				http.Error(w, "bank_statement_id not found", http.StatusNotFound)
				return
			}
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		var acctCurrency sql.NullString
		err = pool.QueryRow(ctx, `SELECT mba.currency FROM public.masterbankaccount mba WHERE mba.account_number = $1 LIMIT 1`, accountNumber).Scan(&acctCurrency)
		if err != nil {
			http.Error(w, "failed to fetch account currency: "+err.Error(), http.StatusInternalServerError)
			return
		}

		var currencyCode string
		if acctCurrency.Valid {
			currencyCode = acctCurrency.String
		}
		rules, err := loadCategoryRuleComponentsPgx(ctx, pool, accountNumber, entityID, currencyCode)
		if err != nil {
			http.Error(w, "failed to fetch category rules: "+err.Error(), http.StatusInternalServerError)
			return
		}

		rows, err := pool.Query(ctx, `
			SELECT transaction_id, tran_id, value_date, transaction_date, description,
			       withdrawal_amount, deposit_amount, balance, raw_json, category_id
			FROM cimplrcorpsaas.bank_statement_transactions
			WHERE bank_statement_id = $1
			ORDER BY value_date, transaction_date, transaction_id
		`, body.BankStatementID)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		categoryCount := map[string]int{}
		debitSum := map[string]float64{}
		creditSum := map[string]float64{}
		uncategorized := []map[string]interface{}{}
		transactionsCount := 0
		totalTxns := 0
		groupedTxns := 0
		ungroupedTxns := 0
		categoryTxns := map[string][]map[string]interface{}{}
		for rows.Next() {
			var transactionID int64
			var tranID sql.NullString
			var valueDate, transactionDate time.Time
			var description string
			var withdrawal, deposit, balance sql.NullFloat64
			var rawJSON json.RawMessage
			var existingCategoryID sql.NullString
			if err := rows.Scan(&transactionID, &tranID, &valueDate, &transactionDate, &description,
				&withdrawal, &deposit, &balance, &rawJSON, &existingCategoryID); err != nil {
				continue
			}
			transactionsCount++
			totalTxns++

			newCategoryID := matchCategoryForTransaction(rules, description, withdrawal, deposit, sql.NullTime{Time: valueDate, Valid: true})

			if (newCategoryID.Valid != existingCategoryID.Valid) || (newCategoryID.Valid && existingCategoryID.Valid && newCategoryID.String != existingCategoryID.String) {
				var catParam interface{}
				if newCategoryID.Valid {
					catParam = newCategoryID.String
				} else {
					catParam = nil
				}
				if _, err := pool.Exec(ctx, `
					UPDATE cimplrcorpsaas.bank_statement_transactions
					SET category_id = $1
					WHERE transaction_id = $2
				`, catParam, transactionID); err != nil {
					logger.LogError("failed to update category_id for transaction_id %d: %v", transactionID, err)
				}
			}

			if newCategoryID.Valid {
				categoryCount[newCategoryID.String]++
				if withdrawal.Valid {
					debitSum[newCategoryID.String] += withdrawal.Float64
				}
				if deposit.Valid {
					creditSum[newCategoryID.String] += deposit.Float64
				}
				groupedTxns++
				catID := newCategoryID.String
				categoryTxns[catID] = append(categoryTxns[catID], map[string]interface{}{
					"transaction_id":    transactionID,
					"tran_id":           tranID.String,
					"value_date":        valueDate,
					"transaction_date":  transactionDate,
					"description":       description,
					"withdrawal_amount": withdrawal.Float64,
					"deposit_amount":    deposit.Float64,
					"balance":           balance.Float64,
					"category_id":       catID,
				})
			} else {
				uncategorized = append(uncategorized, map[string]interface{}{
					"tran_id":          tranID.String,
					"transaction_id":   transactionID,
					"tran_date":        transactionDate,
					"transaction_date": transactionDate,
					"description":      description,
					"value_date":       valueDate,
					"amount":           map[string]interface{}{"withdrawal": withdrawal.Float64, "deposit": deposit.Float64},
					"balance":          balance.Float64,
				})
				ungroupedTxns++
			}
		}

		kpiCats := []map[string]interface{}{}
		foundCategories := []map[string]interface{}{}
		foundCategoryIDs := map[string]bool{}
		for catID, count := range categoryCount {
			var catName string
			for _, rule := range rules {
				if rule.CategoryID == catID {
					catName = rule.CategoryName
					break
				}
			}
			kpiCats = append(kpiCats, map[string]interface{}{
				"category_id":   catID,
				"category_name": catName,
				"count":         count,
				"debit_sum":     debitSum[catID],
				"credit_sum":    creditSum[catID],
				"transactions":  categoryTxns[catID],
			})
			foundCategoryIDs[catID] = true
		}
		for _, rule := range rules {
			if foundCategoryIDs[rule.CategoryID] {
				foundCategories = append(foundCategories, map[string]interface{}{
					"category_id":   rule.CategoryID,
					"category_name": rule.CategoryName,
					"category_type": rule.CategoryType,
				})
				delete(foundCategoryIDs, rule.CategoryID)
			}
		}

		groupedPct := 0.0
		ungroupedPct := 0.0
		if totalTxns > 0 {
			groupedPct = float64(groupedTxns) * 100.0 / float64(totalTxns)
			ungroupedPct = float64(ungroupedTxns) * 100.0 / float64(totalTxns)
		}
		startStr := ""
		endStr := ""
		if !statementPeriodStart.IsZero() {
			startStr = statementPeriodStart.Format(time.RFC3339)
		}
		if !statementPeriodEnd.IsZero() {
			endStr = statementPeriodEnd.Format(time.RFC3339)
		}
		result := map[string]interface{}{
			"pages_processed":                 1,
			"bank_wise_status":                []map[string]interface{}{{"account_number": accountNumber, "status": "SUCCESS"}},
			"statement_date_coverage":         map[string]interface{}{"start": startStr, "end": endStr},
			"category_kpis":                   kpiCats,
			"categories_found":                foundCategories,
			"uncategorized":                   uncategorized,
			"bank_statement_id":               body.BankStatementID,
			"transactions_uploaded_count":     transactionsCount,
			"transactions_under_review_count": 0,
			"transactions_under_review":       []map[string]interface{}{},
			"grouped_transaction_count":       groupedTxns,
			"ungrouped_transaction_count":     ungroupedTxns,
			"grouped_transaction_percent":     groupedPct,
			"ungrouped_transaction_percent":   ungroupedPct,
		}

		apictx.RespondEnvelopeSuccess(w, "Bank statement summary recomputed successfully", result)
	})
}

// loadBankStatementPolicyFields loads the canonical bankStatementRow for bsid
// and maps it to the domain_catalog Fields shape for actionType. On load
// failure it falls back to a row carrying only bsid/entityID (still the full
// canonical key shape, just with zero-valued fields) so every Enforce call
// site sees the same field set whether or not the load succeeded — the
// pre-existing ad hoc helper this replaces had the same fallback behavior.
func loadBankStatementPolicyFields(ctx context.Context, pool *pgxpool.Pool, bsid, entityID, actionType string) map[string]interface{} {
	row, err := loadBankStatementRow(ctx, pool, bsid)
	if err != nil {
		row = bankStatementRow{BankStatementID: bsid, EntityID: entityID}
	}
	return buildBankStatementPolicyFields(row, actionType)
}

// 3. Approve a bank statement (POST, req: user_id, bank_statement_id)
func ApproveBankStatementHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID           string   `json:"user_id"`
			BankStatementIDs []string `json:"bank_statement_ids"`
			Comment          string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || len(body.BankStatementIDs) == 0 {
			http.Error(w, missingUserIDOrBankStatementIDs, http.StatusBadRequest)
			return
		}
		results := make([]map[string]interface{}, 0)
		ctx := r.Context()
		actorName := auditActorDisplayName(ctx, body.UserID)
		for _, bsid := range body.BankStatementIDs {
			var entityID string
			if err := pool.QueryRow(ctx, `SELECT entity_id FROM cimplrcorpsaas.bank_statements WHERE bank_statement_id = $1`, bsid).Scan(&entityID); err == nil {
				if ids := ctxutil.FromContext(ctx).EntityIDs; len(ids) > 0 {
					if !ctxutil.FromContext(ctx).HasEntityAccess(entityID) {
						results = append(results, map[string]interface{}{
							"bank_statement_id": bsid,
							"success":           false,
							"error":             constants.ErrNoAccessToBankStatement,
						})
						continue
					}
				}
			}
			// Check for a pending DELETE first — this must take priority over any RECAT
			// entries that the smart-cat cron may have inserted after the delete request,
			// which would otherwise mask the DELETE intent when querying the latest row.
			var hasPendingDelete bool
			_ = pool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM cimplrcorpsaas.auditactionbankstatement
					WHERE bankstatementid = $1
					  AND actiontype = 'DELETE'
					  AND processing_status = 'PENDING_DELETE_APPROVAL'
				)
			`, bsid).Scan(&hasPendingDelete)

			var actionType, processingStatus string
			if hasPendingDelete {
				actionType = constants.AuditActionDelete
				processingStatus = constants.StatusPendingDeleteApproval
			} else {
				err := pool.QueryRow(ctx, `
					SELECT actiontype, processing_status FROM cimplrcorpsaas.auditactionbankstatement
					WHERE bankstatementid = $1
					  AND actiontype IN ('CREATE', 'EDIT', 'RECAT')
					ORDER BY
						CASE
							WHEN actiontype = 'RECAT' AND processing_status = 'PENDING_EDIT_APPROVAL' THEN 0
							ELSE 1
						END,
						requested_at DESC,
						action_id DESC
					LIMIT 1
				`, bsid).Scan(&actionType, &processingStatus)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
			}
			if actionType == constants.AuditActionDelete && processingStatus == constants.StatusPendingDeleteApproval {
				if out := runtime.EnforceDetailed(ctx, r, pool, runtime.EnforceInput{
					EventCode:           common.TriggerPreApprove,
					ModuleCode:          common.ModuleCash,
					SubModule:           "BANK_STATEMENT",
					EntityCode:          entityID,
					ActorUserID:         body.UserID,
					HandlerName:         "ApproveBankStatementHandler",
					APIPath:             bankStatementApproveRoute,
					DefaultBlockMessage: "Bank statement approval blocked by policy",
					Fields:              loadBankStatementPolicyFields(ctx, pool, bsid, entityID, actionType),
				}); !out.OK {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             out.Message,
						"policy":            out.Result.BlockPayload(),
					})
					continue
				}
				tx, err := pool.Begin(ctx)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				defer tx.Rollback(ctx)
				// _, err = tx.Exec(ctx, `DELETE FROM cimplrcorpsaas.bank_statement_transactions WHERE bank_statement_id = $1`, bsid)
				// _, err = tx.Exec(ctx, `DELETE FROM public.bank_balances_manual WHERE balance_id = $1`, bsid)
				// _, err = tx.Exec(ctx, `DELETE FROM cimplrcorpsaas.bank_statements WHERE bank_statement_id = $1`, bsid)
				_, err = tx.Exec(ctx, `
					UPDATE cimplrcorpsaas.auditactionbankstatement
					SET processing_status = 'APPROVED',
						checker_by = $2,
						checker_at = now(),
						checker_comment = $3,
						checker_ip = $4
					WHERE action_id = (
						SELECT action_id
						FROM cimplrcorpsaas.auditactionbankstatement
						WHERE bankstatementid = $1
						  AND actiontype = 'DELETE'
						  AND processing_status = 'PENDING_DELETE_APPROVAL'
						ORDER BY requested_at DESC, action_id DESC
						LIMIT 1
					)
				`, bsid, actorName, body.Comment, nullIfBlank(apictx.ClientIPFromRequest(r)))
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				_, err = tx.Exec(ctx, `
					UPDATE cimplrcorpsaas.bank_statements
					SET is_deleted = TRUE,
						deleted_at = now(),
						deleted_by = (
							SELECT requested_by
							FROM cimplrcorpsaas.auditactionbankstatement
							WHERE bankstatementid = $1
							  AND actiontype = 'DELETE'
							ORDER BY requested_at DESC, action_id DESC
							LIMIT 1
						)
					WHERE bank_statement_id = $1
				`, bsid)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				_, err = tx.Exec(ctx, `
					UPDATE public.bank_balances_manual
					SET is_deleted = TRUE,
						deleted_at = now(),
						deleted_by = (
							SELECT requested_by
							FROM cimplrcorpsaas.auditactionbankstatement
							WHERE bankstatementid = $1
							  AND actiontype = 'DELETE'
							ORDER BY requested_at DESC, action_id DESC
							LIMIT 1
						)
					WHERE balance_id = $1
				`, bsid)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				dmsjobs.FireDmsEvent(pool, "CASH", "BANK_STATEMENT", "POST_DELETE", []string{bsid}, actorName)
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           true,
					"message":           "Bank statement soft deleted after approval",
				})
			} else {
				if out := runtime.EnforceDetailed(ctx, r, pool, runtime.EnforceInput{
					EventCode:           common.TriggerPreApprove,
					ModuleCode:          common.ModuleCash,
					SubModule:           "BANK_STATEMENT",
					EntityCode:          entityID,
					ActorUserID:         body.UserID,
					HandlerName:         "ApproveBankStatementHandler",
					APIPath:             bankStatementApproveRoute,
					DefaultBlockMessage: "Bank statement approval blocked by policy",
					Fields:              loadBankStatementPolicyFields(ctx, pool, bsid, entityID, actionType),
				}); !out.OK {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             out.Message,
						"policy":            out.Result.BlockPayload(),
					})
					continue
				}
				tx, err := pool.Begin(ctx)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				defer tx.Rollback(ctx)

				var accountNumber string
				var statementPeriodEnd time.Time
				var openingBalance, closingBalance float64
				var uploadS3Key sql.NullString
				err = tx.QueryRow(ctx, `
				       SELECT account_number, statement_period_end, opening_balance, closing_balance, upload_s3_key
				       FROM cimplrcorpsaas.bank_statements
				       WHERE bank_statement_id = $1
			       `, bsid).Scan(&accountNumber, &statementPeriodEnd, &openingBalance, &closingBalance, &uploadS3Key)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				var bankName, currencyCode, nickname, country string
				err = tx.QueryRow(ctx, `
				       SELECT mb.bank_name, mba.currency, COALESCE(mba.account_nickname, mb.bank_name), mba.country
				       FROM public.masterbankaccount mba
				       JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
				       WHERE mba.account_number = $1 AND mba.is_deleted = false
			       `, accountNumber).Scan(&bankName, &currencyCode, &nickname, &country)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				if true {
					if bankName != "" && !ctxutil.FromContext(ctx).HasApprovedBank(bankName) {
						results = append(results, map[string]interface{}{
							"bank_statement_id": bsid,
							"success":           false,
							"error":             constants.ErrBankNotAllowed,
						})
						continue
					}
				}
				if len(ctxutil.FromContext(ctx).BankAccounts) > 0 {
					if !ctxutil.FromContext(ctx).HasApprovedBankAccount(accountNumber) {
						results = append(results, map[string]interface{}{
							"bank_statement_id": bsid,
							"success":           false,
							"error":             "bank account not approved",
						})
						continue
					}
				}
				if !ctxutil.FromContext(ctx).HasApprovedCurrency(currencyCode) {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             constants.ErrCurrencyNotAllowed,
					})
					continue
				}

				var totalCredits, totalDebits float64
				err = tx.QueryRow(ctx, `
				       SELECT COALESCE(SUM(deposit_amount), 0), COALESCE(SUM(withdrawal_amount), 0)
				       FROM cimplrcorpsaas.bank_statement_transactions
				       WHERE bank_statement_id = $1
			       `, bsid).Scan(&totalCredits, &totalDebits)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				_, err = tx.Exec(ctx, `
				       INSERT INTO public.bank_balances_manual (
					       balance_id, bank_name, account_no, currency_code, nickname, country, as_of_date, balance_type, balance_amount, opening_balance, total_credits, total_debits, closing_balance, statement_type, source_channel, upload_s3_key
				       ) VALUES (
					       $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
				       )
				       ON CONFLICT (balance_id) DO UPDATE SET
					       bank_name = EXCLUDED.bank_name,
					       account_no = EXCLUDED.account_no,
					       currency_code = EXCLUDED.currency_code,
					       nickname = EXCLUDED.nickname,
					       country = EXCLUDED.country,
					       as_of_date = EXCLUDED.as_of_date,
					       balance_type = EXCLUDED.balance_type,
					       balance_amount = EXCLUDED.balance_amount,
					       opening_balance = EXCLUDED.opening_balance,
					       total_credits = EXCLUDED.total_credits,
					       total_debits = EXCLUDED.total_debits,
					       closing_balance = EXCLUDED.closing_balance,
					       statement_type = EXCLUDED.statement_type,
					       source_channel = EXCLUDED.source_channel,
					       upload_s3_key = EXCLUDED.upload_s3_key
			       `,
					bsid,
					bankName,
					accountNumber,
					currencyCode,
					nickname,
					country,
					statementPeriodEnd,
					"CLOSING",
					closingBalance,
					openingBalance,
					totalCredits,
					totalDebits,
					closingBalance,
					"BANK_STATEMENT_MANUAL_UPLOAD",
					"MANUAL UPLOAD",
					uploadS3Key,
				)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
					})
					continue
				}

				// Insert bank balance audit as APPROVED immediately — the bank statement was
				// already reviewed and approved by the approver, so the derived balance
				// does not need a separate approval cycle.
				// Use WHERE NOT EXISTS to prevent duplicate audit rows if approval is called twice.
				actionTime := time.Now()
				_, err = tx.Exec(ctx, `
				       INSERT INTO auditactionbankbalances (
					       balance_id, actiontype, processing_status, requested_by, requested_at, requested_ip, checker_by, checker_at, checker_ip, checker_comment
				       )
				       SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
				       WHERE NOT EXISTS (
				           SELECT 1 FROM auditactionbankbalances WHERE balance_id = $11
				       )
				       `,
					bsid,
					constants.AuditActionCreate,
					constants.StatusApproved,
					actorName,
					actionTime,
					nullIfBlank(apictx.ClientIPFromRequest(r)),
					actorName,
					actionTime,
					nullIfBlank(apictx.ClientIPFromRequest(r)),
					body.Comment,
					bsid,
				)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				// _, err = tx.Exec(ctx, `INSERT INTO auditactionbankbalances (
				//        balance_id, actiontype, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment
				// ) SELECT $1, $2, $3, $4, $5, $6, $7, $8 WHERE NOT EXISTS (
				//        SELECT 1 FROM auditactionbankbalances WHERE balance_id = $9 AND actiontype = 'APPROVE'
				// )`, bsid, "APPROVE", constants.StatusApproved, actorName, actionTime, actorName, actionTime, body.Comment, bsid)
				// _, err = tx.Exec(ctx, `INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, bsid, "APPROVE", constants.StatusApproved, actorName, actionTime, actorName, actionTime, body.Comment)
				_, err = tx.Exec(ctx, `
					UPDATE cimplrcorpsaas.auditactionbankstatement
					SET processing_status = 'APPROVED',
						checker_by = $2,
						checker_at = now(),
						checker_comment = $3,
						checker_ip = $4
					WHERE action_id = (
						SELECT action_id
						FROM cimplrcorpsaas.auditactionbankstatement
						WHERE bankstatementid = $1
						  AND actiontype IN ('CREATE', 'EDIT', 'RECAT')
						  AND processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL')
						ORDER BY
							CASE
								WHEN actiontype = 'RECAT' AND processing_status = 'PENDING_EDIT_APPROVAL' THEN 0
								ELSE 1
							END,
							requested_at DESC,
							action_id DESC
						LIMIT 1
					)
				`, bsid, actorName, body.Comment, nullIfBlank(apictx.ClientIPFromRequest(r)))
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				var hasMainStatus bool
				err = tx.QueryRow(ctx, `
					SELECT EXISTS (
						SELECT 1
						FROM information_schema.columns
						WHERE table_schema = 'cimplrcorpsaas'
						  AND table_name = 'bank_statements'
						  AND column_name = 'status'
					)
				`).Scan(&hasMainStatus)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				if hasMainStatus {
					_, err = tx.Exec(ctx, `
						UPDATE cimplrcorpsaas.bank_statements
						SET status = 'APPROVED'
						WHERE bank_statement_id = $1
					`, bsid)
					if err != nil {
						results = append(results, map[string]interface{}{
							"bank_statement_id": bsid,
							"success":           false,
							"error":             err.Error(),
						})
						continue
					}
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				dmsjobs.FireDmsEvent(pool, "CASH", "BANK_STATEMENT", "POST_APPROVE", []string{bsid}, actorName)

				capturedID := bsid
				capturedUser := body.UserID
				payload := BuildBankStatementNotifPayload(context.Background(), pool, []string{capturedID}, "APPROVE", capturedUser)
				go catalog.TriggerNotification(
					context.Background(), pool,
					bankStatementApproveRoute,
					fmt.Sprintf("BSAPPROVE/%s/%d", bsid, time.Now().UnixMilli()),
					payload,
				)

				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           true,
					"message":           "Bank statement approved and bank balance created",
				})
			}
		}
		overallSuccess := len(results) > 0
		for _, result := range results {
			if success, ok := result["success"].(bool); !ok || !success {
				overallSuccess = false
				break
			}
		}
		if overallSuccess {
			apictx.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"results": results})
		} else {
			apictx.RespondEnvelopeFailureCompat(w, http.StatusMultiStatus, "One or more bank statements could not be approved", "", map[string]interface{}{"results": results})
		}
	})
}

// 4. Reject a bank statement (POST, req: user_id, bank_statement_id)
func RejectBankStatementHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID           string   `json:"user_id"`
			BankStatementIDs []string `json:"bank_statement_ids"`
			Comment          string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || len(body.BankStatementIDs) == 0 {
			http.Error(w, missingUserIDOrBankStatementIDs, http.StatusBadRequest)
			return
		}
		results := make([]map[string]interface{}, 0)
		ctx := r.Context()
		actorName := auditActorDisplayName(ctx, body.UserID)
		for _, bsid := range body.BankStatementIDs {
			var entityID string
			if err := pool.QueryRow(ctx, `SELECT entity_id FROM cimplrcorpsaas.bank_statements WHERE bank_statement_id = $1`, bsid).Scan(&entityID); err == nil {
				if ids := ctxutil.FromContext(ctx).EntityIDs; len(ids) > 0 {
					if !ctxutil.FromContext(ctx).HasEntityAccess(entityID) {
						results = append(results, map[string]interface{}{
							"bank_statement_id": bsid,
							"success":           false,
							"error":             constants.ErrNoAccessToBankStatement,
						})
						continue
					}
				}
			}
			// Determine the pending action type the same way ApproveBankStatementHandler
			// does, so the policy check sees the same action_type semantics on Reject
			// as it does on Approve for the same pending row.
			var hasPendingDeleteReject bool
			_ = pool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM cimplrcorpsaas.auditactionbankstatement
					WHERE bankstatementid = $1
					  AND actiontype = 'DELETE'
					  AND processing_status = 'PENDING_DELETE_APPROVAL'
				)
			`, bsid).Scan(&hasPendingDeleteReject)
			var rejectActionType string
			if hasPendingDeleteReject {
				rejectActionType = constants.AuditActionDelete
			} else {
				_ = pool.QueryRow(ctx, `
					SELECT actiontype FROM cimplrcorpsaas.auditactionbankstatement
					WHERE bankstatementid = $1
					  AND actiontype IN ('CREATE', 'EDIT', 'RECAT')
					ORDER BY
						CASE
							WHEN actiontype = 'RECAT' AND processing_status = 'PENDING_EDIT_APPROVAL' THEN 0
							ELSE 1
						END,
						requested_at DESC,
						action_id DESC
					LIMIT 1
				`, bsid).Scan(&rejectActionType)
			}
			if out := runtime.EnforceDetailed(ctx, r, pool, runtime.EnforceInput{
				EventCode:           common.TriggerPreReject,
				ModuleCode:          common.ModuleCash,
				SubModule:           "BANK_STATEMENT",
				EntityCode:          entityID,
				ActorUserID:         body.UserID,
				HandlerName:         "RejectBankStatementHandler",
				APIPath:             "/cash/bank-statements/v2/reject",
				DefaultBlockMessage: "Bank statement rejection blocked by policy",
				Fields:              loadBankStatementPolicyFields(ctx, pool, bsid, entityID, rejectActionType),
			}); !out.OK {
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           false,
					"error":             out.Message,
					"policy":            out.Result.BlockPayload(),
				})
				continue
			}
			tx, err := pool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           false,
					"error":             err.Error(),
				})
				continue
			}
			defer tx.Rollback(ctx)
			// _, err = pool.Exec(ctx, `INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, bsid, "REJECT", constants.StatusRejected, actorName, actionTime, actorName, actionTime, body.Comment)
			_, err = tx.Exec(ctx, `
				UPDATE cimplrcorpsaas.auditactionbankstatement
				SET processing_status = 'REJECTED',
					checker_by = $2,
					checker_at = now(),
					checker_comment = $3,
					checker_ip = $4
				WHERE action_id = (
					SELECT action_id
					FROM cimplrcorpsaas.auditactionbankstatement
					WHERE bankstatementid = $1
					  AND actiontype IN ('CREATE', 'EDIT', 'DELETE', 'RECAT')
					  AND processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL')
					ORDER BY
						CASE
							WHEN actiontype = 'RECAT' AND processing_status = 'PENDING_EDIT_APPROVAL' THEN 0
							ELSE 1
						END,
						requested_at DESC,
						action_id DESC
					LIMIT 1
				)
			`, bsid, actorName, body.Comment, nullIfBlank(apictx.ClientIPFromRequest(r)))
			if err != nil {
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           false,
					"error":             err.Error(),
				})
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           false,
					"error":             err.Error(),
				})
				continue
			}
			results = append(results, map[string]interface{}{
				"bank_statement_id": bsid,
				"success":           true,
				"message":           "Bank statement rejected",
			})
			dmsjobs.FireDmsEvent(pool, "CASH", "BANK_STATEMENT", "POST_REJECT", []string{bsid}, body.UserID)
		}
		overallSuccess := len(results) > 0
		for _, result := range results {
			if success, ok := result["success"].(bool); !ok || !success {
				overallSuccess = false
				break
			}
		}
		if overallSuccess {
			apictx.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"results": results})
		} else {
			apictx.RespondEnvelopeFailureCompat(w, http.StatusMultiStatus, "One or more bank statements could not be rejected", "", map[string]interface{}{"results": results})
		}
		if pool != nil {
			capturedIDs := body.BankStatementIDs
			capturedUser := body.UserID
			capturedComment := body.Comment
			payload := BuildBankStatementNotifPayload(context.Background(), pool, capturedIDs, "REJECT", capturedUser)
			payload["Comment"] = capturedComment
			go catalog.TriggerNotification(
				context.Background(), pool,
				"/cash/bank-statements/v2/reject",
				fmt.Sprintf("BSREJECT/%s/%d", capturedUser, time.Now().UnixMilli()),
				payload,
			)
		}
	})
}

// 5. Delete bank statement (POST, req: user_id, bank_statement_id)
func DeleteBankStatementHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID           string   `json:"user_id"`
			BankStatementIDs []string `json:"bank_statement_ids"`
			Comment          string   `json:"comment"`
			Reason           string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || len(body.BankStatementIDs) == 0 {
			http.Error(w, missingUserIDOrBankStatementIDs, http.StatusBadRequest)
			return
		}
		results := make([]map[string]interface{}, 0)
		ctx := r.Context()
		for _, bsid := range body.BankStatementIDs {
			var entityID string
			if err := pool.QueryRow(ctx, `SELECT entity_id FROM cimplrcorpsaas.bank_statements WHERE bank_statement_id = $1`, bsid).Scan(&entityID); err == nil {
				if ids := ctxutil.FromContext(ctx).EntityIDs; len(ids) > 0 {
					if !ctxutil.FromContext(ctx).HasEntityAccess(entityID) {
						results = append(results, map[string]interface{}{
							"bank_statement_id": bsid,
							"success":           false,
							"error":             constants.ErrNoAccessToBankStatement,
						})
						continue
					}
				}
			}
			var latestActionType, latestProcessingStatus string
			latestAuditErr := pool.QueryRow(ctx, `
				SELECT actiontype, processing_status
				FROM cimplrcorpsaas.auditactionbankstatement
				WHERE bankstatementid = $1
				  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
				ORDER BY requested_at DESC, action_id DESC
				LIMIT 1
			`, bsid).Scan(&latestActionType, &latestProcessingStatus)
			if latestAuditErr == nil && latestActionType == constants.AuditActionDelete && latestProcessingStatus == constants.StatusPendingDeleteApproval {
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           false,
					"error":             "delete request already pending approval",
				})
				continue
			}
			deleteComment := body.Comment
			if strings.TrimSpace(deleteComment) == "" {
				deleteComment = body.Reason
			}
			out := runtime.EnforceDetailed(ctx, r, pool, runtime.EnforceInput{
				EventCode:           common.TriggerPreDelete,
				ModuleCode:          common.ModuleCash,
				SubModule:           "BANK_STATEMENT",
				EntityCode:          entityID,
				ActorUserID:         body.UserID,
				HandlerName:         "DeleteBankStatementHandler",
				APIPath:             "/cash/bank-statements/v2/delete",
				DefaultBlockMessage: "Bank statement delete blocked by policy",
				Fields:              loadBankStatementPolicyFields(ctx, pool, bsid, entityID, constants.AuditActionDelete),
			})
			if !out.OK {
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           false,
					"error":             out.Message,
					"policy":            out.Result.BlockPayload(),
				})
				continue
			}
			requestedBy := auditActorDisplayName(ctx, body.UserID)
			_, err := pool.Exec(ctx, `
				       INSERT INTO cimplrcorpsaas.auditactionbankstatement (
					       bankstatementid, actiontype, processing_status, requested_by, requested_at, requested_ip, reason
				       ) VALUES ($1, $2, $3, $4, $5, $6, $7)
			       `, bsid, constants.AuditActionDelete, constants.StatusPendingDeleteApproval, requestedBy, time.Now().UTC(), nullIfBlank(apictx.ClientIPFromRequest(r)), deleteComment)
			if err != nil {
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           false,
					"error":             err.Error(),
				})
				continue
			}
			approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
				ModuleCode:          common.ModuleCash,
				TransactionType:     "BANK_STATEMENT_DELETE",
				RecordID:            bsid,
				RecordTable:         "cimplrcorpsaas.bank_statements",
				AuditTable:          "cimplrcorpsaas.auditactionbankstatement",
				AuditIDColumn:       "bankstatementid",
				ActionType:          "DELETE",
				Amount:              0,
				SubmittedBy:         body.UserID,
				SubmittedByEmail:    requestedBy,
				MatrixID:            out.Result.TriggerApprovalMatrixID,
				RequirePinnedMatrix: true,
				AutoApplyIfUnpinned: false,
			})
			results = append(results, map[string]interface{}{
				"bank_statement_id": bsid,
				"success":           true,
				"message":           "Delete request submitted for approval",
			})
		}
		overallSuccess := len(results) > 0
		for _, result := range results {
			if success, ok := result["success"].(bool); !ok || !success {
				overallSuccess = false
				break
			}
		}
		if overallSuccess {
			apictx.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"results": results})
		} else {
			apictx.RespondEnvelopeFailureCompat(w, http.StatusMultiStatus, "One or more bank statements could not be queued for deletion", "", map[string]interface{}{"results": results})
		}
		if pool != nil {
			capturedIDs := body.BankStatementIDs
			capturedUser := body.UserID
			capturedComment := body.Comment
			if strings.TrimSpace(capturedComment) == "" {
				capturedComment = body.Reason
			}
			go catalog.TriggerNotification(
				context.Background(), pool,
				"/cash/bank-statements/v2/delete",
				fmt.Sprintf("BSDELETE/%s/%d", capturedUser, time.Now().UnixMilli()),
				map[string]interface{}{
					"BankStatementIDs": capturedIDs,
					"Count":            len(capturedIDs),
					"UserID":           capturedUser,
					"Comment":          capturedComment,
					"Action":           constants.StatusPendingDeleteApproval,
					"ActionAt":         time.Now().Format(time.RFC3339),
				},
			)
		}
	})
}

func UploadBankStatementV2Handler(pgxPool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			apictx.RespondEnvelopeError(w, http.StatusMethodNotAllowed, "Only POST method is allowed for this endpoint.", "")
			return
		}

		isPDF := r.URL.Query().Get("is_pdf") == "true"

		uploadActor := strings.TrimSpace(r.FormValue("user_id"))
		if uploadActor == "" {
			uploadActor = requestedByFromCtx(r.Context(), "")
		}
		enforceBankStatementUpload := func(fields map[string]interface{}) bool {
			uploadAccount, _ := fields["account_number"].(string)
			uploadEntityID := lookupEntityIDForAccount(r.Context(), pgxPool, uploadAccount)
			fields["entity_id"] = uploadEntityID
			ok, _ := runtime.EnforceWithMatrix(r.Context(), w, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreUpload,
				ModuleCode:          common.ModuleCash,
				SubModule:           "BANK_STATEMENT",
				EntityCode:          uploadEntityID,
				ActorUserID:         uploadActor,
				HandlerName:         "UploadBankStatementV2Handler",
				APIPath:             "/cash/upload-bank-statement",
				DefaultBlockMessage: "Bank statement upload blocked by policy",
				Fields:              fields,
			})
			return ok
		}

		if isPDF {
			logger.LogInfo("[BANK_STATEMENT] PDF flag detected, processing from bank.json")
			if !enforceBankStatementUpload(map[string]interface{}{"upload_type": "pdf"}) {
				return
			}
			result, err := ProcessBankStatementFromJSON(r.Context(), pgxPool, apictx.ClientIPFromRequest(r))
			if err != nil {
				apictx.RespondEnvelopeError(w, http.StatusInternalServerError, userFriendlyUploadError(err), "")
				return
			}

			msg := "Bank statement from PDF uploaded successfully"
			if rc, ok := result["transactions_under_review_count"].(int); ok && rc > 0 {
				msg = fmt.Sprintf("Bank statement from PDF uploaded successfully. %d transactions are under review.", rc)
			}

			apictx.RespondEnvelopeSuccess(w, msg, result)
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			logger.LogError("[BANK-UPLOAD-ERROR] Failed to parse multipart form: %v", err)
			apictx.RespondEnvelopeError(w, http.StatusBadRequest, "Unable to read the uploaded file. Please try again.", "")
			return
		}

		multiFlag := r.FormValue("multi") == "true"
		if multiFlag {
			if !enforceBankStatementUpload(map[string]interface{}{"upload_type": "multi"}) {
				return
			}
			// explicit multi=true: only try multi approach, surface error if it fails
			UploadMultiAccountBankStatementHandler(pgxPool).ServeHTTP(w, r)
			return
		}
		// No explicit multi flag: run normal single-account V2 first.
		// Multi is only attempted as a last-resort fallback if V2 fails.

		useMappingFlag := strings.ToLower(strings.TrimSpace(r.FormValue("mapping")))
		useMapping := useMappingFlag == "true" || useMappingFlag == "1"

		var mappings *ColumnMappings
		if useMapping {
			mappingsJSON := r.FormValue("column_mappings")
			if mappingsJSON != "" {
				mappings = &ColumnMappings{}
				if err := json.Unmarshal([]byte(mappingsJSON), mappings); err != nil {
					logger.LogError("[BANK-UPLOAD-ERROR] Invalid column_mappings JSON: %v", err)
					apictx.RespondEnvelopeError(w, http.StatusBadRequest, "Invalid column_mappings JSON: "+err.Error(), "")
					return
				}
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Custom column mappings provided: %+v", mappings)
			} else {
				apictx.RespondEnvelopeError(w, http.StatusBadRequest, "mapping flag is true but column_mappings not provided", "")
				return
			}

			if strings.TrimSpace(mappings.AccountNumber) == "" {
				apictx.RespondEnvelopeError(w, http.StatusBadRequest, "Column-Mappings must include 'Account Number' when mapping is enabled", "")
				return
			}
		}

		var fileFieldsAvailable []string
		if r.MultipartForm != nil && r.MultipartForm.File != nil {
			for fieldName := range r.MultipartForm.File {
				fileFieldsAvailable = append(fileFieldsAvailable, fieldName)
			}
			logger.LogInfo("[BANK-UPLOAD-DEBUG] Available file form fields: %v", fileFieldsAvailable)
		} else {
			logger.LogError("[BANK-UPLOAD-ERROR] No file fields found in multipart form - request may not include a file attachment")
		}

		file, fileHeader, err := r.FormFile("file")
		uploadFileName := ""
		if fileHeader != nil {
			uploadFileName = fileHeader.Filename
		}
		if err != nil {
			logger.LogError("[BANK-UPLOAD-ERROR] FormFile('file') error: %v", err)
			if file == nil {
				file, fileHeader, err = r.FormFile("statement")
				if fileHeader != nil {
					uploadFileName = fileHeader.Filename
				}
			}
			if err != nil && file == nil {
				file, fileHeader, err = r.FormFile("bankStatement")
				if fileHeader != nil {
					uploadFileName = fileHeader.Filename
				}
			}
			if err != nil && file == nil {
				if r.MultipartForm != nil && len(r.MultipartForm.File) > 0 {
					logger.LogInfo("[BANK-UPLOAD-DEBUG] Trying first available file field from: %v", fileFieldsAvailable)
					for fieldName, files := range r.MultipartForm.File {
						if len(files) > 0 {
							logger.LogInfo("[BANK-UPLOAD-DEBUG] Using field: %s", fieldName)
							file, err = files[0].Open()
							uploadFileName = files[0].Filename
							break
						}
					}
				}
			}
			if err != nil || file == nil {
				errorMsg := "File not found in request. Please attach a bank statement file using the 'file' field in form-data."
				if len(fileFieldsAvailable) > 0 {
					errorMsg = fmt.Sprintf("No 'file' field found. Available fields: %v. Please use 'file' as the field name.", fileFieldsAvailable)
				}
				apictx.RespondEnvelopeError(w, http.StatusBadRequest, errorMsg, "")
				return
			}
		}
		defer file.Close()

		fileBytes, err := io.ReadAll(file)
		if err != nil {
			apictx.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to read the uploaded file. Please try again.", "")
			return
		}

		// Auto-detect multi-account statements even when multi=true is not provided.
		// This keeps preview/upload behavior aligned with user expectation for mixed-account CSVs.
		if !multiFlag {
			if isLikelyMultiAccountStatement(uploadFileName, fileBytes) {
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Auto-detected multi-account statement for %s; routing to multi handler", uploadFileName)
				UploadMultiAccountBankStatementHandler(pgxPool).ServeHTTP(w, r)
				return
			}
		}
		hash := sha256.Sum256(fileBytes)
		fileHash := fmt.Sprintf("%x", hash[:])

		fileReader := bytes.NewReader(fileBytes)
		mf := &bytesFile{Reader: fileReader}

		// Parse account number overrides from form-data.
		// account_numbers is always an array (JSON array, comma-separated, or repeated field).
		//
		// force_override=true  + 1 account  → assign that account to the file unconditionally
		// force_override=true  + N>1        → error: single file can only belong to one account
		// force_override=true  + 0 accounts → error: must provide account number
		// force_override=false + N accounts → weighted scoring; single account used as fallback
		// force_override=false + 0 accounts → auto-detect from file content against DB
		accountNumbers := parseAccountNumbers(r.MultipartForm.Value)
		forceOverride := r.FormValue("force_override") == "true"
		accountOverride := ""

		if forceOverride {
			switch len(accountNumbers) {
			case 0:
				apictx.RespondEnvelopeError(w, http.StatusBadRequest, "force_override=true requires at least one account number in account_numbers", "")
				return
			case 1:
				accountOverride = accountNumbers[0]
				logger.LogError("[BANK-UPLOAD-DEBUG] force_override=true — using account %s directly", accountOverride)
			default:
				apictx.RespondEnvelopeError(w, http.StatusBadRequest, fmt.Sprintf("force_override=true with a single file requires exactly 1 account number, got %d. Use the zip endpoint for multiple files.", len(accountNumbers)), "")
				return
			}
		} else if len(accountNumbers) >= 1 {
			// Weighted scoring: match filename patterns + masked numbers in file content.
			var fileContent [][]string
			if parsedRows, parseErr := parseFileToRows(fileBytes); parseErr == nil {
				fileContent = parsedRows
			}
			matched := matchAccountNumberToFile(r.Context(), pgxPool, uploadFileName, "", accountNumbers, fileContent)
			if matched != "" {
				accountOverride = matched
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Matched file to account %s via weighted scoring", matched)
			} else if len(accountNumbers) == 1 {
				accountOverride = accountNumbers[0]
				logger.LogError("[BANK-UPLOAD-DEBUG] Single account fallback: %s", accountOverride)
			} else {
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Multiple accounts, no weighted match found for %s — relying on file content extraction", uploadFileName)
			}
		}
		policyAccount := accountOverride
		if policyAccount == "" {
			var policyFileContent [][]string
			if parsedRows, parseErr := parseFileToRows(fileBytes); parseErr == nil {
				policyFileContent = parsedRows
			}
			policyAccount = matchAccountNumberToFile(r.Context(), pgxPool, uploadFileName, "", nil, policyFileContent)
			if policyAccount == "" {
				policyAccount = lookupAccountNumberFromContent(r.Context(), pgxPool, policyFileContent)
			}
		}
		if !enforceBankStatementUpload(map[string]interface{}{
			"upload_file_name": uploadFileName,
			"account_number":   policyAccount,
			"use_mapping":      useMapping,
		}) {
			return
		}
		result, err := UploadBankStatementV2WithCategorization(
			r.Context(),
			pgxPool,
			mf,
			fileHash,
			UploadOpts{
				UseMapping:            useMapping,
				Mappings:              mappings,
				AccountNumberOverride: accountOverride,
				UploadFileName:        uploadFileName,
				UploadedBy:            requestedByFromCtx(r.Context(), r.FormValue("user_id")),
				RequestedIP:           apictx.ClientIPFromRequest(r),
				Password:              r.FormValue("password"),
				PgxPool:               pgxPool,
			},
		)
		if err != nil {
			// V2 failed — try multi as a last-resort fallback (e.g. multi-account sheet)
			logger.LogError("[BANK-UPLOAD-DEBUG] V2 failed (%v); trying multi handler as fallback", err)
			multiRec := httptest.NewRecorder()
			UploadMultiAccountBankStatementHandler(pgxPool).ServeHTTP(multiRec, r)
			// Multi handler returns outer "success":true even when ALL individual accounts fail.
			// We must check that at least one account in data{} actually succeeded.
			var multiResp struct {
				Success bool                       `json:"success"`
				Data    map[string]json.RawMessage `json:"data"`
			}
			multiActuallySucceeded := false
			if multiRec.Code == http.StatusOK && json.Unmarshal(multiRec.Body.Bytes(), &multiResp) == nil && multiResp.Success {
				for _, raw := range multiResp.Data {
					var acct struct {
						Success bool `json:"success"`
					}
					if json.Unmarshal(raw, &acct) == nil && acct.Success {
						multiActuallySucceeded = true
						break
					}
				}
			}
			if multiActuallySucceeded {
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Multi fallback succeeded for at least one account")
				for k, v := range multiRec.Header() {
					w.Header()[k] = v
				}
				w.WriteHeader(multiRec.Code)
				w.Write(multiRec.Body.Bytes())
				return
			}
			// Both V2 and multi failed.
			// Check whether the multi per-account errors carry a more informative message
			// (e.g. "statement already uploaded") that should be surfaced instead of V2's
			// generic "bank account not found" — which would be misleading to the user.
			surfaceMsg := userFriendlyUploadError(err)
			if multiRec.Code == http.StatusOK {
				var multiResp2 struct {
					Success bool                       `json:"success"`
					Data    map[string]json.RawMessage `json:"data"`
				}
				if json.Unmarshal(multiRec.Body.Bytes(), &multiResp2) == nil && len(multiResp2.Data) > 0 {
					var multiMsgs []string
					allAlreadyStored := true
					for _, raw := range multiResp2.Data {
						var acct struct {
							Success bool   `json:"success"`
							Message string `json:"message"`
						}
						if json.Unmarshal(raw, &acct) == nil && !acct.Success {
							msg := acct.Message
							if msg == "" {
								allAlreadyStored = false
								continue
							}
							msgLower := strings.ToLower(msg)
							if !strings.Contains(msgLower, "already") && !strings.Contains(msgLower, "exist") && !strings.Contains(msgLower, "duplicate") {
								allAlreadyStored = false
							}
							// Collect unique messages
							duplicate := false
							for _, m := range multiMsgs {
								if m == msg {
									duplicate = true
									break
								}
							}
							if !duplicate {
								multiMsgs = append(multiMsgs, msg)
							}
						}
					}
					// If every failing account reported an "already stored" style error, surface
					// those messages — they are more actionable than V2's account-not-found error.
					if allAlreadyStored && len(multiMsgs) > 0 {
						surfaceMsg = strings.Join(multiMsgs, " | ")
						logger.LogError("[BANK-UPLOAD-DEBUG] Surfacing multi 'already stored' error instead of V2 error: %s", surfaceMsg)
					}
				}
			}
			logger.LogError("[BANK-UPLOAD-DEBUG] Multi fallback also failed; returning error: %s", surfaceMsg)
			status := http.StatusInternalServerError
			if errors.Is(err, ErrStatementPeriodExists) || errors.Is(err, ErrFileAlreadyUploaded) ||
				errors.Is(err, ErrAllTransactionsDuplicate) ||
				strings.Contains(strings.ToLower(surfaceMsg), "already uploaded") ||
				strings.Contains(strings.ToLower(surfaceMsg), "already exist") {
				status = http.StatusConflict
			}
			apictx.RespondEnvelopeError(w, status, surfaceMsg, "")
			return
		}

		msg := "Bank statement uploaded successfully"
		if rc, ok := result["transactions_under_review_count"].(int); ok && rc > 0 {
			msg = fmt.Sprintf("Bank statement uploaded successfully. %d transactions are under review.", rc)
		}

		apictx.RespondEnvelopeSuccess(w, msg, result)

		// DMS POST_UPLOAD is fired inside UploadBankStatementV2WithCategorization
		// (covers single-file and ZIP paths). Notification stays here for the HTTP route.
		if pgxPool != nil {
			capturedResult := result
			capturedUser := r.FormValue("user_id")
			capturedFile := uploadFileName
			go func() {
				notifPayload := BuildBankStatementPayloadFromV2Result(
					capturedResult,
					capturedUser,
					capturedFile,
					"PREVIEW",
				)
				bsID := notifPayload.BankStatementID
				if bsID == "" {
					if v, ok := capturedResult["bank_statement_id"].(string); ok {
						bsID = v
					}
				}
				catalog.TriggerNotification(
					context.Background(), pgxPool,
					"/cash/preview",
					fmt.Sprintf("BSUPLOAD/%s/%d", bsID, time.Now().UnixMilli()),
					notifPayload.ToMap(),
				)
			}()
		}
	})
}

func isLikelyMultiAccountStatement(filename string, fileBytes []byte) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	if ext != ".csv" && ext != ".xlsx" && ext != ".xls" {
		return false
	}
	rows, err := parseFileToRows(fileBytes)
	if err != nil || len(rows) < 2 {
		return false
	}

	header := rows[0]
	findIdx := func(keywords ...string) int {
		for i, h := range header {
			lc := strings.ToLower(strings.TrimSpace(h))
			for _, kw := range keywords {
				if strings.Contains(lc, strings.ToLower(kw)) {
					return i
				}
			}
		}
		return -1
	}

	accIdx := findIdx("account number", "account_no", "account no", "acct_no", "acct no")
	if accIdx == -1 {
		return false
	}

	uniq := map[string]struct{}{}
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		if accIdx >= len(row) {
			continue
		}
		acc := strings.TrimSpace(row[accIdx])
		if acc == "" {
			continue
		}
		uniq[acc] = struct{}{}
		if len(uniq) > 1 {
			return true
		}
	}
	return false
}

// UploadZippedBankStatementsHandler accepts a zip file containing multiple bank statement files,
// unzips them, processes each file one by one, and returns aggregated results.
func UploadZippedBankStatementsHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Cap whole request slightly above max zip (5 MiB) so metadata fields still fit.
		if err := r.ParseMultipartForm(8 << 20); err != nil {
			http.Error(w, fmt.Sprintf("Failed to parse multipart form: %v", err), http.StatusBadRequest)
			return
		}

		userID := r.FormValue("user_id")
		if userID == "" {
			http.Error(w, constants.ErrMissingUserID, http.StatusBadRequest)
			return
		}

		zipFile, zipHeader, err := r.FormFile("file")
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get zip file: %v", err), http.StatusBadRequest)
			return
		}
		defer zipFile.Close()

		useMapping := r.FormValue("useMapping") == "true"
		var mappings *ColumnMappings
		if useMapping {
			mappingsJSON := r.FormValue("mappings")
			if mappingsJSON != "" {
				mappings = &ColumnMappings{}
				if err := json.Unmarshal([]byte(mappingsJSON), mappings); err != nil {
					http.Error(w, fmt.Sprintf("Invalid mappings JSON: %v", err), http.StatusBadRequest)
					return
				}
			}
		}

		// Parse account number overrides from form-data
		accountNumbers := parseAccountNumbers(r.MultipartForm.Value)
		// force_override semantics:
		//   force=true + 0 accounts  → trust each file's own filename digits as the account number (no DB check during routing)
		//   force=true + 1 account   → assign ALL zip files to that one account
		//   force=true + N accounts  → 1:1 positional mapping: file[0]→account[0], file[1]→account[1], ...
		//                               if len(accounts) != len(files) → error
		//   force=false + N accounts → match each file to one account via weighted scoring (filename+content); unmatched → error
		//   force=false + 0 accounts → auto-detect account per file from filename/content against DB
		forceOverride := r.FormValue("force_override") == "true"
		logger.LogInfo("[ZIP-UPLOAD] Received %d account number(s) from form-data, force_override=%v", len(accountNumbers), forceOverride)

		zipData, err := readBankStatementZipBytes(zipFile, zipHeader)
		if err != nil {
			if strings.Contains(err.Error(), "exceeds the maximum size") {
				http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, fmt.Sprintf("Failed to read zip file: %v", err), http.StatusInternalServerError)
			return
		}

		zipReader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to open zip file: %v", err), http.StatusBadRequest)
			return
		}

		type FileResult struct {
			FileName string                 `json:"file_name"`
			Success  bool                   `json:"success"`
			Result   map[string]interface{} `json:"result,omitempty"`
			Error    string                 `json:"error,omitempty"`
		}

		results := []FileResult{}
		successCount := 0
		failureCount := 0

		// --- Collect all processable file entries first so we can validate counts ---
		type zipEntry struct {
			name     string
			data     []byte
			fileHash string
		}
		var fileEntries []zipEntry
		for _, zipFileEntry := range zipReader.File {
			if zipFileEntry.FileInfo().IsDir() {
				continue
			}
			base := filepath.Base(zipFileEntry.Name)
			dir := filepath.Dir(zipFileEntry.Name)
			if strings.HasPrefix(base, ".") || strings.HasPrefix(base, "._") ||
				strings.Contains(dir, "__MACOSX") || base == ".DS_Store" {
				continue
			}
			ext := strings.ToLower(filepath.Ext(zipFileEntry.Name))
			if ext != ".xlsx" && ext != ".xls" && ext != ".csv" && ext != ".numbers" {
				results = append(results, FileResult{
					FileName: zipFileEntry.Name,
					Success:  false,
					Error:    fmt.Sprintf("Unsupported file type: %s (only .xlsx, .xls, .csv, .numbers allowed)", ext),
				})
				failureCount++
				continue
			}
			fr, oErr := zipFileEntry.Open()
			if oErr != nil {
				results = append(results, FileResult{FileName: zipFileEntry.Name, Success: false, Error: fmt.Sprintf("Failed to open file from zip: %v", oErr)})
				failureCount++
				continue
			}
			fd, rErr := io.ReadAll(fr)
			fr.Close()
			if rErr != nil {
				results = append(results, FileResult{FileName: zipFileEntry.Name, Success: false, Error: fmt.Sprintf("Failed to read file contents: %v", rErr)})
				failureCount++
				continue
			}
			h := sha256.New()
			h.Write(fd)
			fileEntries = append(fileEntries, zipEntry{name: zipFileEntry.Name, data: fd, fileHash: fmt.Sprintf("%x", h.Sum(nil))})
		}

		// Validate force + N-accounts case: must be 1:1
		if forceOverride && len(accountNumbers) > 1 && len(accountNumbers) != len(fileEntries) {
			http.Error(w, fmt.Sprintf(
				"force_override=true with %d account numbers but zip contains %d processable files — counts must match for 1:1 mapping",
				len(accountNumbers), len(fileEntries),
			), http.StatusBadRequest)
			return
		}

		for fileIdx, ze := range fileEntries {
			base := filepath.Base(ze.name)

			// Determine account override for this specific file.
			accountOverride := ""

			switch {
			case forceOverride && len(accountNumbers) > 1:
				// 1:1 positional mapping — already validated counts above
				accountOverride = accountNumbers[fileIdx]
				logger.LogError("[ZIP-UPLOAD] force+N: file[%d] %s → account %s", fileIdx, ze.name, accountOverride)

			case forceOverride && len(accountNumbers) == 1:
				// All files → single account
				accountOverride = accountNumbers[0]
				logger.LogError("[ZIP-UPLOAD] force+1: assigning file %s → account %s", ze.name, accountOverride)

			case forceOverride && len(accountNumbers) == 0:
				// Trust filename: extract account number segment from the filename.
				// Supports both pure-digit accounts ("0036013656") and dash-containing accounts ("0-456789-678").
				isDigits := func(s string) bool {
					if len(s) == 0 {
						return false
					}
					for _, c := range s {
						if c < '0' || c > '9' {
							return false
						}
					}
					return true
				}
				isDashAccount := func(s string) bool {
					// digits-and-dashes, starts+ends with digit, length >= 7
					if len(s) < 7 || s[0] < '0' || s[0] > '9' || s[len(s)-1] < '0' || s[len(s)-1] > '9' {
						return false
					}
					for _, c := range s {
						if (c < '0' || c > '9') && c != '-' {
							return false
						}
					}
					return true
				}
				// Pass 1: split on _ . space only (preserves dashes) — catches "0-456789-678"
				for _, part := range strings.FieldsFunc(base, func(r rune) bool {
					return r == '_' || r == '.' || r == ' '
				}) {
					// strip extension suffix just in case
					part = strings.TrimSuffix(strings.TrimSuffix(part, ".xls"), ".xlsx")
					if len(part) >= 7 && (isDigits(part) || isDashAccount(part)) {
						accountOverride = part
						break
					}
				}
				// Pass 2: also split on dashes for plain digit accounts
				if accountOverride == "" {
					for _, part := range strings.FieldsFunc(base, func(r rune) bool {
						return r == '_' || r == '-' || r == '.' || r == ' '
					}) {
						if len(part) >= 7 && isDigits(part) {
							accountOverride = part
							break
						}
					}
				}
				if accountOverride == "" {
					// fallback: scan character-by-character for a 7+ digit run
					start := -1
					for i, c := range base {
						if c >= '0' && c <= '9' {
							if start < 0 {
								start = i
							}
						} else {
							if start >= 0 && i-start >= 7 {
								accountOverride = base[start:i]
								break
							}
							start = -1
						}
					}
				}
				if accountOverride == "" {
					results = append(results, FileResult{
						FileName: ze.name, Success: false,
						Error: fmt.Sprintf("force_override=true but no account number digits found in filename '%s'", base),
					})
					failureCount++
					continue
				}
				logger.LogInfo("[ZIP-UPLOAD] force+filename: file %s → account %s (from filename)", ze.name, accountOverride)

			case !forceOverride && len(accountNumbers) > 0:
				// Match each file to one of the provided accounts via weighted scoring
				var fileContent [][]string
				if parsedRows, parseErr := parseFileToRows(ze.data); parseErr == nil {
					fileContent = parsedRows
				}
				matched := matchAccountNumberToFile(ctx, pool, ze.name, "", accountNumbers, fileContent)
				if matched != "" {
					accountOverride = matched
					logger.LogInfo("[ZIP-UPLOAD] matched: file %s → account %s", ze.name, matched)
				} else if len(accountNumbers) == 1 {
					accountOverride = accountNumbers[0]
					logger.LogError("[ZIP-UPLOAD] single-account fallback: file %s → account %s", ze.name, accountOverride)
				} else {
					results = append(results, FileResult{
						FileName: ze.name, Success: false,
						Error: fmt.Sprintf("Could not match file '%s' to any of the provided account numbers. Add the account number to the filename or use force_override=true.", base),
					})
					failureCount++
					continue
				}

			default: // !forceOverride && len(accountNumbers) == 0
				// Auto-detect: UploadBankStatementV2 resolves account from filename+content against DB
				logger.LogInfo("[ZIP-UPLOAD] auto-detect: file %s — no account hint, resolving from filename/content", ze.name)
			}

			bytesReader := bytes.NewReader(ze.data)
			file := &bytesFile{Reader: bytesReader}

			result, err := UploadBankStatementV2WithCategorization(
				ctx,
				pool,
				file,
				ze.fileHash,
				UploadOpts{
					UseMapping:            useMapping,
					Mappings:              mappings,
					AccountNumberOverride: accountOverride,
					UploadFileName:        ze.name,
					UploadedBy:            requestedByFromCtx(ctx, r.FormValue("user_id")),
					RequestedIP:           apictx.ClientIPFromRequest(r),
					PgxPool:               pool,
				},
			)
			if err != nil {
				results = append(results, FileResult{
					FileName: ze.name,
					Success:  false,
					Error:    userFriendlyUploadError(err),
				})
				failureCount++
				continue
			}

			results = append(results, FileResult{
				FileName: ze.name,
				Success:  true,
				Result:   result,
			})
			successCount++
		}

		zipMsg := fmt.Sprintf("Processed %d files from zip", len(results))
		apictx.RespondEnvelopeSuccessCompat(w, zipMsg, map[string]interface{}{
			"zip_file_name": zipHeader.Filename,
			"total_files":   len(results),
			"success_count": successCount,
			"failure_count": failureCount,
			"results":       results,
			"uploaded_by":   userID,
			"upload_time":   time.Now().Format(time.RFC3339),
		})

		if pool != nil {
			for i := range results {
				fres := results[i]
				if !fres.Success || fres.Result == nil {
					continue
				}
				capturedResult := fres.Result
				capturedZipFile := zipHeader.Filename
				go func() {
					notifPayload := BuildBankStatementPayloadFromV2Result(
						capturedResult,
						userID,
						capturedZipFile,
						constants.StatusPendingApproval,
					)
					catalog.TriggerNotification(context.Background(), pool,
						"/cash/upload-bank-statement-zip",
						fmt.Sprintf("BSUPLOAD/%s/%d", notifPayload.BankStatementID, time.Now().UnixMilli()),
						notifPayload.ToMap(),
					)
				}()
			}
		}
	})
}
