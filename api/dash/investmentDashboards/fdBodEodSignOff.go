// Package investmentdashboards — BOD/EOD persistence endpoints.
//
// Three POST endpoints back the persona dashboard:
//   /dash/investment/fd/bod-eod/signoff       — per-item sign-off (auto-creates run header)
//   /dash/investment/fd/bod-eod/handover      — append handover note to current run
//   /dash/investment/fd/bod-eod/bank-contacts — upsert manual bank-contact row
//
// All three are idempotent and tolerant: callers can replay safely.
package investmentdashboards

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── shared helpers ───────────────────────────────────────────────────────────

// upsertBodEodRun returns the run_id, creating the row when missing. Sign-off
// and handover both depend on a stable run header keyed by entity+date+mode.
func upsertBodEodRun(
	ctx context.Context,
	pool *pgxpool.Pool,
	entityID, businessDate, mode, userID string,
) (int64, error) {
	const q = `
		INSERT INTO cimplr.fd_bod_eod_run (entity_id, business_date, mode, opened_by)
		VALUES ($1, $2::date, $3, NULLIF($4, ''))
		ON CONFLICT (entity_id, business_date, mode)
		DO UPDATE SET opened_at = cimplr.fd_bod_eod_run.opened_at  -- no-op, returns row
		RETURNING run_id`
	var runID int64
	err := pool.QueryRow(ctx, q, entityID, businessDate, mode, userID).Scan(&runID)
	return runID, err
}

// resolveBusinessDate accepts YYYY-MM-DD, defaults to today UTC when blank.
func resolveBusinessDate(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Now().UTC().Format(constants.DateFormat)
	}
	return s
}

func normalizeMode(s string) string {
	m := strings.ToUpper(strings.TrimSpace(s))
	if m != "BOD" && m != "EOD" {
		return "BOD"
	}
	return m
}

// ─── 1. sign-off endpoint ────────────────────────────────────────────────────

type bodEodSignOffReq struct {
	UserID       string `json:"user_id"`
	EntityID     string `json:"entity_id"`
	BusinessDate string `json:"business_date"`
	Mode         string `json:"mode"`
	Persona      string `json:"persona"`
	ItemCode     string `json:"item_code"`
	IsDone       bool   `json:"is_done"`
	Remarks      string `json:"remarks"`
}

// PostFDBodEodSignOff toggles a single checklist item for a (run, persona).
// Auto-creates the run header. user_id comes from prevalidation context when
// the body field is blank (typical browser flow via nos).
func PostFDBodEodSignOff(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}
		var req bodEodSignOffReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		ctx := r.Context()
		userID := strings.TrimSpace(req.UserID)
		if userID == "" {
			userID = middlewares.GetUserIDFromContext(ctx)
		}
		entityID := strings.TrimSpace(req.EntityID)
		if entityID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_id is required")
			return
		}
		if strings.TrimSpace(req.ItemCode) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "item_code is required")
			return
		}
		persona := normalizePersona(req.Persona)
		mode := normalizeMode(req.Mode)
		businessDate := resolveBusinessDate(req.BusinessDate)

		runID, err := upsertBodEodRun(ctx, pool, entityID, businessDate, mode, userID)
		if err != nil {
			api.LogError("[BodEodDashV2] signoff: upsert run failed: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "failed to open run")
			return
		}

		const upsert = `
			INSERT INTO cimplr.fd_bod_eod_sign_off
			  (run_id, persona, item_code, is_done, done_by, done_at, remarks)
			VALUES ($1, $2, $3, $4, NULLIF($5,''), CASE WHEN $4 THEN now() ELSE NULL END, NULLIF($6,''))
			ON CONFLICT (run_id, persona, item_code) DO UPDATE
			   SET is_done = EXCLUDED.is_done,
			       done_by = EXCLUDED.done_by,
			       done_at = EXCLUDED.done_at,
			       remarks = EXCLUDED.remarks`
		if _, err := pool.Exec(ctx, upsert,
			runID, persona, req.ItemCode, req.IsDone, userID, req.Remarks,
		); err != nil {
			api.LogError("[BodEodDashV2] signoff: upsert item failed: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "failed to save sign-off")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":        runID,
			"persona":       persona,
			"mode":          mode,
			"item_code":     req.ItemCode,
			"is_done":       req.IsDone,
			"business_date": businessDate,
			"entity_id":     entityID,
		})
	}
}

// ─── 2. handover-note endpoint ───────────────────────────────────────────────

type bodEodHandoverReq struct {
	UserID       string   `json:"user_id"`
	EntityID     string   `json:"entity_id"`
	BusinessDate string   `json:"business_date"`
	Mode         string   `json:"mode"`
	Note         string   `json:"note"`
	Tags         []string `json:"tags"`
}

// PostFDBodEodHandover appends a free-text handover note for the current run.
func PostFDBodEodHandover(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}
		var req bodEodHandoverReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		ctx := r.Context()
		userID := strings.TrimSpace(req.UserID)
		if userID == "" {
			userID = middlewares.GetUserIDFromContext(ctx)
		}
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id is required")
			return
		}
		entityID := strings.TrimSpace(req.EntityID)
		if entityID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_id is required")
			return
		}
		note := strings.TrimSpace(req.Note)
		if note == "" {
			api.RespondWithError(w, http.StatusBadRequest, "note is required")
			return
		}
		mode := normalizeMode(req.Mode)
		businessDate := resolveBusinessDate(req.BusinessDate)

		runID, _ := upsertBodEodRun(ctx, pool, entityID, businessDate, mode, userID)

		const ins = `
			INSERT INTO cimplr.fd_bod_eod_handover_note
			  (run_id, entity_id, business_date, mode, note, tags, created_by)
			VALUES (NULLIF($1,0), $2, $3::date, $4, $5, $6, $7)
			RETURNING note_id, TO_CHAR(created_at, 'YYYY-MM-DD"T"HH24:MI:SS')`
		var noteID int64
		var createdAt string
		if err := pool.QueryRow(ctx, ins,
			runID, entityID, businessDate, mode, note, req.Tags, userID,
		).Scan(&noteID, &createdAt); err != nil {
			api.LogError("[BodEodDashV2] handover insert failed: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "failed to save handover note")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"note_id":    noteID,
			"created_at": createdAt,
			"created_by": userID,
		})
	}
}

// ─── 3. bank-contact upsert endpoint ─────────────────────────────────────────

type bodEodBankContactReq struct {
	UserID      string `json:"user_id"`
	ContactID   int64  `json:"contact_id"` // 0 → insert, >0 → update
	EntityID    string `json:"entity_id"`
	BankID      string `json:"bank_id"`
	BankName    string `json:"bank_name"`
	ReasonCode  string `json:"reason_code"`
	ReferenceID string `json:"reference_id"`
	AssignedTo  string `json:"assigned_to"`
	DueAt       string `json:"due_at"` // ISO timestamp
	Status      string `json:"status"`
}

// UpsertFDBodEodBankContact inserts or updates a single bank-contact target
// for the persona "Banks to contact today" widget.
func UpsertFDBodEodBankContact(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}
		var req bodEodBankContactReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		entityID := strings.TrimSpace(req.EntityID)
		if entityID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_id is required")
			return
		}
		status := strings.ToUpper(strings.TrimSpace(req.Status))
		if status == "" {
			status = "OPEN"
		}
		ctx := r.Context()

		// New row.
		if req.ContactID == 0 {
			const ins = `
				INSERT INTO cimplr.fd_bod_eod_bank_contact
				  (entity_id, bank_id, bank_name, reason_code, reference_id,
				   assigned_to, due_at, status)
				VALUES ($1, NULLIF($2,''), NULLIF($3,''), NULLIF($4,''), NULLIF($5,''),
				        NULLIF($6,''), NULLIF($7::text,'')::timestamptz, $8)
				RETURNING contact_id`
			var id int64
			if err := pool.QueryRow(ctx, ins,
				entityID, req.BankID, req.BankName, req.ReasonCode, req.ReferenceID,
				req.AssignedTo, req.DueAt, status,
			).Scan(&id); err != nil {
				api.LogError("[BodEodDashV2] bank-contact insert: %v", err)
				api.RespondWithError(w, http.StatusInternalServerError, "failed to insert bank contact")
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"contact_id": id,
			})
			return
		}

		// Update existing.
		const upd = `
			UPDATE cimplr.fd_bod_eod_bank_contact SET
			  bank_id      = NULLIF($2,''),
			  bank_name    = NULLIF($3,''),
			  reason_code  = NULLIF($4,''),
			  reference_id = NULLIF($5,''),
			  assigned_to  = NULLIF($6,''),
			  due_at       = NULLIF($7::text,'')::timestamptz,
			  status       = $8,
			  closed_at    = CASE WHEN $8 = 'DONE' THEN now() ELSE closed_at END
			WHERE contact_id = $1 AND COALESCE(is_deleted,false) = false`
		tag, err := pool.Exec(ctx, upd,
			req.ContactID, req.BankID, req.BankName, req.ReasonCode, req.ReferenceID,
			req.AssignedTo, req.DueAt, status,
		)
		if err != nil {
			api.LogError("[BodEodDashV2] bank-contact update: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "failed to update bank contact")
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"contact_id": req.ContactID,
			"updated":    tag.RowsAffected(),
		})
	}
}
