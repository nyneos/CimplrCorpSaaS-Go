package bankstatement

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/ctxutil"
	cat "CimplrCorpSaas/internal/services/categorizer"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────
// GET REVIEW QUEUE
// POST /cash/smart-cat/review-queue
// Returns low-confidence / unallocated transactions for analyst review.
// ─────────────────────────────────────────────────────────────

func GetReviewQueueHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id,omitempty"`
			Status   string `json:"status,omitempty"` // PENDING | CONFIRMED | CORRECTED | DISMISSED
			Limit    int    `json:"limit,omitempty"`
			Offset   int    `json:"offset,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			http.Error(w, constants.ErrMissingUserID, http.StatusBadRequest)
			return
		}
		if req.Status == "" {
			req.Status = "PENDING"
		}
		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		entityIDs := scope.EntityIDs
		if len(entityIDs) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusUnauthorized)
			return
		}
		accountNumbers := scopeValues(scope.BankAccounts, "account_number")

		// LATERAL picks at most one masterbankaccount row so nickname/bank
		// JOINs cannot fan-out a single queue item into multiple rows
		// (which inflates Account / Bank / Nickname filter & group counts).
		qStr := `
			SELECT
				q.queue_id, q.transaction_id, q.suggested_cat,
				q.confidence::FLOAT8,
				q.step, q.status, q.created_at,
				bs.account_number,
				COALESCE(t.description, ''),
				COALESCE(t.narration_clean, ''),
				COALESCE(t.narration_ref, ''),
				COALESCE(t.payment_channel, ''),
				t.withdrawal_amount::FLOAT8, t.deposit_amount::FLOAT8,
				t.transaction_date,
				bs.entity_id,
				COALESCE(mba.account_nickname, bs.account_number),
				COALESCE(mc.category_name, ''),
				COALESCE(t.classification_step, ''),
				COALESCE(t.confidence_score::FLOAT8, 0),
				bs.bank_statement_id::text,
				COALESCE(e.entity_name, ''),
				COALESCE(mb.bank_name, ''),
				bs.statement_period_start,
				bs.statement_period_end
			FROM cimplrcorpsaas.categorization_review_queue q
			JOIN cimplrcorpsaas.bank_statement_transactions t
			    ON t.transaction_id = q.transaction_id
			JOIN cimplrcorpsaas.bank_statements bs
			    ON bs.bank_statement_id = t.bank_statement_id
			LEFT JOIN public.masterentitycash e ON e.entity_id = bs.entity_id
			LEFT JOIN LATERAL (
				SELECT mba.account_nickname, mba.bank_id
				FROM public.masterbankaccount mba
				WHERE mba.account_number = bs.account_number
				  AND COALESCE(mba.is_deleted, false) = false
				ORDER BY CASE WHEN mba.entity_id = bs.entity_id THEN 0 ELSE 1 END,
				         mba.account_nickname NULLS LAST
				LIMIT 1
			) mba ON TRUE
			LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
			LEFT JOIN public.mastercashflowcategory mc ON mc.category_id::text = q.suggested_cat
			WHERE q.status = $1
			  AND COALESCE(bs.is_deleted, false) = false
			  AND bs.entity_id = ANY($2)
			  AND (cardinality($3::text[]) = 0 OR bs.account_number = ANY($3::text[]))`

		args := []interface{}{req.Status, entityIDs, accountNumbers}
		n := 4
		if req.EntityID != "" {
			if !scope.HasEntityAccess(req.EntityID) {
				http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusUnauthorized)
				return
			}
			qStr += ` AND bs.entity_id = $` + strconv.Itoa(n)
			args = append(args, req.EntityID)
			n++
		}
		if req.Limit > 0 {
			qStr += ` ORDER BY q.created_at DESC LIMIT $` + strconv.Itoa(n) + ` OFFSET $` + strconv.Itoa(n+1)
			args = append(args, req.Limit, req.Offset)
		} else {
			qStr += ` ORDER BY q.created_at DESC`
			if req.Offset > 0 {
				qStr += ` OFFSET $` + strconv.Itoa(n)
				args = append(args, req.Offset)
			}
		}

		rows, err := pool.Query(ctx, qStr, args...)
		if err != nil {
			writeErrJSON(w, "query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		seenQueue := make(map[int64]struct{})
		for rows.Next() {
			var queueID, txnID int64
			var suggestedCat *string
			var confidence *float64
			var step, status string
			var createdAt time.Time
			var accountNumber, description, narrationClean, narrationRef, paymentChannel string
			var withdrawal, deposit *float64
			var transactionDate *time.Time
			var entityID, accountNickname, suggestedCatName string
			var txnClassStep string
			var txnConfidence float64
			var bankStatementID, entityName, bankName string
			var periodStart, periodEnd *time.Time

			if err := rows.Scan(
				&queueID, &txnID, &suggestedCat, &confidence, &step, &status, &createdAt,
				&accountNumber, &description, &narrationClean, &narrationRef, &paymentChannel,
				&withdrawal, &deposit, &transactionDate,
				&entityID, &accountNickname, &suggestedCatName,
				&txnClassStep, &txnConfidence,
				&bankStatementID, &entityName, &bankName, &periodStart, &periodEnd,
			); err != nil {
				fmt.Printf("[REVIEW-QUEUE] scan error: %v\n", err)
				continue
			}
			if _, dup := seenQueue[queueID]; dup {
				continue
			}
			seenQueue[queueID] = struct{}{}

			accountNumber = strings.TrimSpace(accountNumber)
			accountNickname = strings.TrimSpace(accountNickname)
			entityID = strings.TrimSpace(entityID)
			entityName = strings.TrimSpace(entityName)
			bankName = strings.TrimSpace(bankName)
			bankStatementID = strings.TrimSpace(bankStatementID)
			paymentChannel = strings.TrimSpace(paymentChannel)

			// If the transaction has not yet been through narration processing
			// (e.g. uploaded before the advisory-lock fix), derive narration
			// in-memory so the queue always returns a useful narration_clean.
			if narrationClean == "" && description != "" {
				nr := cat.ProcessNarration(description)
				narrationClean = nr.Clean
				if narrationRef == "" {
					narrationRef = nr.Ref
				}
				if paymentChannel == "" {
					paymentChannel = string(nr.Channel)
				}
				// Persist asynchronously so subsequent fetches already have it.
				go func(txID int64, nc, ns, ref string, pc cat.IndianBankChannel) {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					_, _ = pool.Exec(ctx, `
						UPDATE cimplrcorpsaas.bank_statement_transactions
						SET narration_clean=$1, narration_stemmed=$2,
						    narration_ref=$3, payment_channel=$4
						WHERE transaction_id=$5
						  AND (narration_clean IS NULL OR narration_clean = '')
					`, nc, ns, ref, string(pc), txID)
				}(txnID, nr.Clean, nr.Stemmed, nr.Ref, nr.Channel)
			}

			// current_step / current_confidence reflect the transaction's
			// actual latest classification — not the frozen queue snapshot.
			// After a manual CORRECTION or CONFIRMATION, these will differ
			// from q.step / q.confidence and the UI should use them for display.
			currentStep := txnClassStep
			if currentStep == "" {
				currentStep = step // fall back to queue snapshot
			}
			currentConf := txnConfidence
			if currentConf == 0 && confidence != nil {
				currentConf = *confidence // fall back to queue snapshot
			}

			row := map[string]interface{}{
				"queue_id":           queueID,
				"transaction_id":     txnID,
				"suggested_cat":      suggestedCat,
				"suggested_cat_name": suggestedCatName,
				"confidence":         confidence,
				"step":               step,
				"current_step":       currentStep,
				"current_confidence": currentConf,
				"status":             status,
				"created_at":         createdAt.Format(time.RFC3339),
				"account_number":     accountNumber,
				"account_nickname":   accountNickname,
				"description":        description,
				"narration_clean":    narrationClean,
				"narration_ref":      narrationRef,
				"payment_channel":    paymentChannel,
				"withdrawal":         withdrawal,
				"deposit":            deposit,
				"entity_id":          entityID,
				"bank_statement_id":  bankStatementID,
				"entity_name":        entityName,
				"bank_name":          bankName,
			}
			if transactionDate != nil {
				row["transaction_date"] = transactionDate.Format(constants.DateFormat)
			}
			if periodStart != nil {
				row["statement_period_start"] = periodStart.Format(constants.DateFormat)
			}
			if periodEnd != nil {
				row["statement_period_end"] = periodEnd.Format(constants.DateFormat)
			}
			out = append(out, row)
		}

		// Count must use the same scope filters as the main query so orphaned /
		// out-of-scope queue rows are not included in the total.
		cntStr := `
			SELECT COUNT(*)
			FROM cimplrcorpsaas.categorization_review_queue q
			JOIN cimplrcorpsaas.bank_statement_transactions t ON t.transaction_id = q.transaction_id
			JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
			WHERE q.status = $1
			  AND COALESCE(bs.is_deleted, false) = false
			  AND bs.entity_id = ANY($2)
			  AND (cardinality($3::text[]) = 0 OR bs.account_number = ANY($3::text[]))`
		cntArgs := []interface{}{req.Status, entityIDs, accountNumbers}
		cn := 4
		if req.EntityID != "" {
			cntStr += ` AND bs.entity_id = $` + strconv.Itoa(cn)
			cntArgs = append(cntArgs, req.EntityID)
		}
		var total int
		_ = pool.QueryRow(ctx, cntStr, cntArgs...).Scan(&total)

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"rows":   out,
			"total":  total,
			"limit":  req.Limit,
			"offset": req.Offset,
		})
	})
}

// ─────────────────────────────────────────────────────────────
// REVIEW ACTION
// POST /cash/smart-cat/review-action
// action: CONFIRM | CORRECT | DISMISS
// ─────────────────────────────────────────────────────────────

func ReviewActionHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			UserID        string `json:"user_id"`
			TransactionID int64  `json:"transaction_id"`
			Action        string `json:"action"`
			CategoryID    string `json:"category_id"` // only for CORRECT
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidJSONPrefix+err.Error(), http.StatusBadRequest)
			return
		}
		if req.UserID == "" || req.TransactionID == 0 || req.Action == "" {
			http.Error(w, "user_id, transaction_id, and action are required", http.StatusBadRequest)
			return
		}
		if req.Action == "CORRECT" && req.CategoryID == "" {
			http.Error(w, "category_id is required for CORRECT action", http.StatusBadRequest)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		reviewedBy := req.UserID
		if session != nil && session.Name != "" {
			reviewedBy = session.Name
		}

		ctx := r.Context()
		var opErr error

		switch req.Action {
		case "CONFIRM":
			// If the caller supplies a category_id (e.g. the UI is applying an
			// AI bulk-suggest result), skip reading from the queue and use the
			// provided category directly with standard human-confirmation confidence.
			catToConfirm := req.CategoryID
			conf := 0.90
			sourceRef := "analyst_confirm"

			if catToConfirm == "" {
				// Classic confirm: read the engine's suggestion from the queue.
				var suggestedCat *string
				var suggestedConf *float64
				var suggestedStep string
				fetchErr := pool.QueryRow(ctx, `
					SELECT suggested_cat, confidence::FLOAT8, COALESCE(step,'')
					FROM cimplrcorpsaas.categorization_review_queue
					WHERE transaction_id = $1
				`, req.TransactionID).Scan(&suggestedCat, &suggestedConf, &suggestedStep)
				if fetchErr != nil {
					writeErrJSON(w, "failed to fetch queue entry: "+fetchErr.Error(), http.StatusInternalServerError)
					return
				}
				if suggestedCat == nil || *suggestedCat == "" {
					writeErrJSON(w, "no suggested category to confirm", http.StatusBadRequest)
					return
				}
				catToConfirm = *suggestedCat
				if suggestedConf != nil && *suggestedConf > conf {
					conf = *suggestedConf
				}
			} else {
				// AI-assisted confirm: user applied an AI suggestion.
				sourceRef = "ai_assisted_confirm"
			}

			if !enforceSmartCatReview(ctx, w, r, pool, req.TransactionID, catToConfirm,
				common.TriggerPreApprove, req.UserID, "Review confirm blocked by policy") {
				return
			}

			confirmTx, txErr := pool.Begin(ctx)
			if txErr != nil {
				writeErrJSON(w, "begin tx: "+txErr.Error(), http.StatusInternalServerError)
				return
			}
			defer confirmTx.Rollback(ctx) //nolint:errcheck

			// 1. Write category + mark as human-confirmed on the transaction
			var confirmTag pgconn.CommandTag
			if confirmTag, opErr = confirmTx.Exec(ctx, `
				UPDATE cimplrcorpsaas.bank_statement_transactions
				SET category_id=$1,
				    classification_step='CONFIRMATION',
				    confidence_score=$2
				WHERE transaction_id=$3
			`, catToConfirm, conf, req.TransactionID); opErr != nil {
				break
			}
			if confirmTag.RowsAffected() == 0 {
				opErr = fmt.Errorf("transaction %d not found", req.TransactionID)
				break
			}
			// 2. Immutable audit log entry — classified_by = analyst name
			if _, opErr = confirmTx.Exec(ctx, `
				INSERT INTO cimplrcorpsaas.classification_audit_log
				    (transaction_id, category_id, confidence, classification_step, source_ref, classified_by)
				VALUES ($1, $2, $3, 'CONFIRMATION', $4, $5)
			`, req.TransactionID, catToConfirm, conf, sourceRef, reviewedBy); opErr != nil {
				break
			}
			// 3. Mark queue entry confirmed; also update step so the frozen
			//    snapshot stays in sync with the transaction's actual state.
			_, opErr = confirmTx.Exec(ctx, `
				UPDATE cimplrcorpsaas.categorization_review_queue
				SET status='CONFIRMED', step='CONFIRMATION',
				    reviewed_by=$1, reviewed_at=now()
				WHERE transaction_id=$2
			`, reviewedBy, req.TransactionID)
			if opErr == nil {
				opErr = confirmTx.Commit(ctx)
			}

		case "CORRECT":
			opErr = applyCorrection(ctx, pool, req.TransactionID, req.CategoryID, reviewedBy)

		case "DISMISS":
			var dismissedCat *string
			if err := pool.QueryRow(ctx, `
				SELECT suggested_cat
				FROM cimplrcorpsaas.categorization_review_queue
				WHERE transaction_id = $1
			`, req.TransactionID).Scan(&dismissedCat); err != nil {
				writeErrJSON(w, "failed to fetch queue entry: "+err.Error(), http.StatusInternalServerError)
				return
			}
			dismissedCatID := ""
			if dismissedCat != nil {
				dismissedCatID = *dismissedCat
			}
			if !enforceSmartCatReview(ctx, w, r, pool, req.TransactionID, dismissedCatID,
				common.TriggerPreReject, req.UserID, "Review dismiss blocked by policy") {
				return
			}
			_, opErr = pool.Exec(ctx, `
				UPDATE cimplrcorpsaas.categorization_review_queue
				SET status='DISMISSED', reviewed_by=$1, reviewed_at=now()
				WHERE transaction_id=$2
			`, reviewedBy, req.TransactionID)

		default:
			http.Error(w, "action must be CONFIRM, CORRECT, or DISMISS", http.StatusBadRequest)
			return
		}

		if opErr != nil {
			writeErrJSON(w, opErr.Error(), http.StatusInternalServerError)
			return
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"transaction_id": req.TransactionID,
			"action":         req.Action,
		})
	})
}

// ─────────────────────────────────────────────────────────────
// SAVE ANALYST CORRECTION  (can be called directly, without queue)
// POST /cash/smart-cat/correction
// ─────────────────────────────────────────────────────────────

func SaveCorrectionHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			UserID        string `json:"user_id"`
			TransactionID int64  `json:"transaction_id"`
			CategoryID    string `json:"category_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
			req.UserID == "" || req.TransactionID == 0 || req.CategoryID == "" {
			http.Error(w, "user_id, transaction_id, and category_id are required", http.StatusBadRequest)
			return
		}

		session := middlewares.GetSessionFromContext(r.Context())
		correctedBy := req.UserID
		if session != nil && session.Name != "" {
			correctedBy = session.Name
		}

		if err := applyCorrection(r.Context(), pool, req.TransactionID, req.CategoryID, correctedBy); err != nil {
			writeErrJSON(w, err.Error(), http.StatusInternalServerError)
			return
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"transaction_id": req.TransactionID,
			"category_id":    req.CategoryID,
		})
	})
}

// ─────────────────────────────────────────────────────────────
// GL → Category Mapping
// POST /cash/smart-cat/gl-mapping/create
// ─────────────────────────────────────────────────────────────

func CreateGLMappingHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			UserID      string `json:"user_id"`
			GLAccountID string `json:"gl_account_id"`
			CategoryID  string `json:"category_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
			req.GLAccountID == "" || req.CategoryID == "" {
			http.Error(w, "gl_account_id and category_id are required", http.StatusBadRequest)
			return
		}
		session := middlewares.GetSessionFromContext(r.Context())
		createdBy := req.UserID
		if session != nil && session.Name != "" {
			createdBy = session.Name
		}
		if _, err := pool.Exec(r.Context(), `
			INSERT INTO cimplrcorpsaas.gl_category_mapping (gl_account_id, category_id, created_by)
			VALUES ($1, $2, $3)
			ON CONFLICT (gl_account_id) DO UPDATE SET category_id = EXCLUDED.category_id
		`, req.GLAccountID, req.CategoryID, createdBy); err != nil {
			writeErrJSON(w, "insert failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		api.RespondEnvelopeSuccess(w, "Success", nil)
	})
}

// ─────────────────────────────────────────────────────────────
// applyCorrection — core logic used by both ReviewActionHandler
// and SaveCorrectionHandler.
//
// Atomically (inside a single pgx transaction):
//  1. Reads narration_clean / narration_stemmed from the transaction row.
//     If not yet processed, derives them with ProcessNarration.
//  2. Updates bank_statement_transactions.category_id.
//  3. Inserts a correction record → feeds Step 4 for all future runs.
//  4. Appends an immutable audit log entry (classified_by = human).
//  5. Marks categorization_review_queue entry as CORRECTED.
// ─────────────────────────────────────────────────────────────

func applyCorrection(ctx context.Context, pool *pgxpool.Pool, txnID int64, categoryID, correctedBy string) error {
	// Read current narration columns
	var narrationClean, narrationStemmed, entityID, rawDesc string
	err := pool.QueryRow(ctx, `
		SELECT
			COALESCE(t.narration_clean, ''),
			COALESCE(t.narration_stemmed, ''),
			COALESCE(bs.entity_id, ''),
			COALESCE(t.description, '')
		FROM cimplrcorpsaas.bank_statement_transactions t
		LEFT JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
		WHERE t.transaction_id = $1
	`, txnID).Scan(&narrationClean, &narrationStemmed, &entityID, &rawDesc)
	if err != nil {
		return fmt.Errorf("fetch transaction: %w", err)
	}

	// Derive narration if not yet processed by the categorizer
	if narrationClean == "" {
		nr := cat.ProcessNarration(rawDesc)
		narrationClean = nr.Clean
		narrationStemmed = nr.Stemmed
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// 1. Update transaction
	updateTag, err := tx.Exec(ctx, `
		UPDATE cimplrcorpsaas.bank_statement_transactions
		SET category_id=$1, classification_step='CORRECTION', confidence_score=0.99
		WHERE transaction_id=$2
	`, categoryID, txnID)
	if err != nil {
		return fmt.Errorf("update category: %w", err)
	}
	if updateTag.RowsAffected() == 0 {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	// 2. Save correction for Step 4 future lookups.
	// ON CONFLICT: same narration_clean → update with newest correction so
	// lookupCorrection always finds the most-recent human decision.
	if _, err := tx.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.categorization_corrections
		    (narration_clean, narration_stemmed, category_id, corrected_by, transaction_id, entity_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (narration_clean)
		DO UPDATE SET
		    category_id      = EXCLUDED.category_id,
		    narration_stemmed = EXCLUDED.narration_stemmed,
		    corrected_by     = EXCLUDED.corrected_by,
		    corrected_at     = now(),
		    transaction_id   = EXCLUDED.transaction_id,
		    entity_id        = EXCLUDED.entity_id,
		    is_active        = TRUE
	`, narrationClean, narrationStemmed, categoryID, correctedBy, txnID, entityID); err != nil {
		return fmt.Errorf("save correction: %w", err)
	}

	// 3. Immutable audit log — classified_by = corrected_by (human name)
	if _, err := tx.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.classification_audit_log
		    (transaction_id, category_id, confidence, classification_step, source_ref, classified_by)
		VALUES ($1, $2, 0.99, 'CORRECTION', 'manual_correction', $3)
	`, txnID, categoryID, correctedBy); err != nil {
		return fmt.Errorf("audit log: %w", err)
	}

	// 4. Update review queue if the transaction is in it; also update step so
	// the frozen snapshot stays in sync with the transaction's actual state.
	_, _ = tx.Exec(ctx, `
		UPDATE cimplrcorpsaas.categorization_review_queue
		SET status='CORRECTED', step='CORRECTION',
		    reviewed_by=$1, reviewed_at=now()
		WHERE transaction_id=$2
	`, correctedBy, txnID)

	return tx.Commit(ctx)
}

// writeErrJSON writes a JSON error response with the given status code.
func writeErrJSON(w http.ResponseWriter, msg string, code int) {
	api.RespondEnvelopeError(w, code, msg, "")
}
