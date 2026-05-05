package bankstatement

import (
	"CimplrCorpSaas/api/constants"
	middlewares "CimplrCorpSaas/api/middlewares"
	cat "CimplrCorpSaas/internal/services/categorizer"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

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
		if req.Limit <= 0 || req.Limit > 500 {
			req.Limit = 100
		}

		ctx := r.Context()
		qStr := `
			SELECT
				q.queue_id, q.transaction_id, q.suggested_cat, q.confidence,
				q.step, q.status, q.created_at,
				bs.account_number,
				COALESCE(t.description, ''),
				COALESCE(t.narration_clean, ''),
				COALESCE(t.narration_ref, ''),
				COALESCE(t.payment_channel, ''),
				t.withdrawal_amount, t.deposit_amount, t.value_date,
				bs.entity_id,
				COALESCE(mba.account_nickname, bs.account_number),
				COALESCE(mc.category_name, '')
			FROM cimplrcorpsaas.categorization_review_queue q
			JOIN cimplrcorpsaas.bank_statement_transactions t
			    ON t.transaction_id = q.transaction_id
			JOIN cimplrcorpsaas.bank_statements bs
			    ON bs.bank_statement_id = t.bank_statement_id
			LEFT JOIN public.masterbankaccount mba ON mba.account_number = bs.account_number
			LEFT JOIN public.mastercashflowcategory mc ON mc.category_id = q.suggested_cat
			WHERE q.status = $1`

		args := []interface{}{req.Status}
		n := 2
		if req.EntityID != "" {
			qStr += ` AND bs.entity_id = $` + strconv.Itoa(n)
			args = append(args, req.EntityID)
			n++
		}
		qStr += ` ORDER BY q.created_at DESC LIMIT $` + strconv.Itoa(n) + ` OFFSET $` + strconv.Itoa(n+1)
		args = append(args, req.Limit, req.Offset)

		rows, err := pool.Query(ctx, qStr, args...)
		if err != nil {
			writeErrJSON(w, "query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var queueID, txnID int64
			var suggestedCat *string
			var confidence *float64
			var step, status string
			var createdAt time.Time
			var accountNumber, description, narrationClean, narrationRef, paymentChannel string
			var withdrawal, deposit *float64
			var valueDate *time.Time
			var entityID, accountNickname, suggestedCatName string

			if err := rows.Scan(
				&queueID, &txnID, &suggestedCat, &confidence, &step, &status, &createdAt,
				&accountNumber, &description, &narrationClean, &narrationRef, &paymentChannel,
				&withdrawal, &deposit, &valueDate,
				&entityID, &accountNickname, &suggestedCatName,
			); err != nil {
				fmt.Printf("[REVIEW-QUEUE] scan error: %v\n", err)
				continue
			}

			row := map[string]interface{}{
				"queue_id":           queueID,
				"transaction_id":     txnID,
				"suggested_cat":      suggestedCat,
				"suggested_cat_name": suggestedCatName,
				"confidence":         confidence,
				"step":               step,
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
			}
			if valueDate != nil {
				row["value_date"] = valueDate.Format("2006-01-02")
			}
			out = append(out, row)
		}

		var total int
		_ = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM cimplrcorpsaas.categorization_review_queue WHERE status = $1`,
			req.Status,
		).Scan(&total)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"rows":    out,
			"total":   total,
			"limit":   req.Limit,
			"offset":  req.Offset,
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
			_, opErr = pool.Exec(ctx, `
				UPDATE cimplrcorpsaas.categorization_review_queue
				SET status='CONFIRMED', reviewed_by=$1, reviewed_at=now()
				WHERE transaction_id=$2
			`, reviewedBy, req.TransactionID)

		case "CORRECT":
			opErr = applyCorrection(ctx, pool, req.TransactionID, req.CategoryID, reviewedBy)

		case "DISMISS":
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

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
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

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
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
	if _, err := tx.Exec(ctx, `
		UPDATE cimplrcorpsaas.bank_statement_transactions
		SET category_id=$1, classification_step='CORRECTION', confidence_score=0.99
		WHERE transaction_id=$2
	`, categoryID, txnID); err != nil {
		return fmt.Errorf("update category: %w", err)
	}

	// 2. Save correction for Step 4 future lookups
	if _, err := tx.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.categorization_corrections
		    (narration_clean, narration_stemmed, category_id, corrected_by, transaction_id, entity_id)
		VALUES ($1, $2, $3, $4, $5, $6)
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

	// 4. Update review queue if the transaction is in it
	_, _ = tx.Exec(ctx, `
		UPDATE cimplrcorpsaas.categorization_review_queue
		SET status='CORRECTED', reviewed_by=$1, reviewed_at=now()
		WHERE transaction_id=$2
	`, correctedBy, txnID)

	return tx.Commit(ctx)
}

// writeErrJSON writes a JSON error response with the given status code.
func writeErrJSON(w http.ResponseWriter, msg string, code int) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": msg})
}
