package sweepconfig

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CreateSweepConfiguration inserts a sweep configuration and creates a CREATE audit action (PENDING_APPROVAL)
func CreateSweepConfiguration(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID        string   `json:"user_id"`
			SweepID       string   `json:"sweep_id,omitempty"`
			EntityName    string   `json:"entity_name,omitempty"`
			BankName      string   `json:"bank_name,omitempty"`
			BankAccount   string   `json:"bank_account,omitempty"`
			SweepType     string   `json:"sweep_type,omitempty"`
			ParentAccount string   `json:"parent_account,omitempty"`
			BufferAmount  *float64 `json:"buffer_amount,omitempty"`
			Frequency     string   `json:"frequency,omitempty"`
			CutoffTime    string   `json:"cutoff_time,omitempty"`
			AutoSweep     string   `json:"auto_sweep,omitempty"`
			ActiveStatus  string   `json:"active_status,omitempty"`
			Reason        string   `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIDRequired)
			return
		}

		// resolve requested_by
		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		sweepID := req.SweepID
		if sweepID == "" {
			sweepID = fmt.Sprintf("SWEEP-%d", time.Now().UnixNano()%1000000)
		}

		ins := `INSERT INTO mastersweepconfiguration (sweep_id, entity_name, bank_name, bank_account, sweep_type, parent_account, buffer_amount, frequency, cutoff_time, auto_sweep, active_status, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now(),now())`
		_, err := pgxPool.Exec(ctx, ins,
			sweepID,
			nullifyEmpty(req.EntityName),
			nullifyEmpty(req.BankName),
			nullifyEmpty(req.BankAccount),
			nullifyEmpty(req.SweepType),
			nullifyEmpty(req.ParentAccount),
			nullifyFloat(req.BufferAmount),
			nullifyEmpty(req.Frequency),
			nullifyEmpty(req.CutoffTime),
			nullifyEmpty(req.AutoSweep),
			nullifyEmpty(req.ActiveStatus),
		)
		if err != nil {
			api.RespondWithResult(w, false, "failed to insert sweep configuration: "+err.Error())
			return
		}

		auditQ := `INSERT INTO auditactionsweepconfiguration (sweep_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,now())`
		if _, err := pgxPool.Exec(ctx, auditQ, sweepID, nullifyEmpty(req.Reason), requestedBy); err != nil {
			api.RespondWithResult(w, false, "failed to create audit action: "+err.Error())
			return
		}

		api.RespondWithResult(w, true, sweepID)
	}
}

// UpdateSweepConfiguration updates allowed fields for a specific sweep_id and creates an EDIT audit (PENDING_EDIT_APPROVAL)
func UpdateSweepConfiguration(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string                 `json:"user_id"`
			SweepID string                 `json:"sweep_id"`
			Fields  map[string]interface{} `json:"fields"`
			Reason  string                 `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" || req.SweepID == "" {
			api.RespondWithResult(w, false, "user_id and sweep_id required")
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "failed to begin tx: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		sel := `SELECT entity_name, bank_name, bank_account, sweep_type, parent_account, buffer_amount, frequency, cutoff_time, auto_sweep, active_status FROM mastersweepconfiguration WHERE sweep_id=$1 FOR UPDATE`
		var curEntity, curBank, curAccount, curType, curParent, curFrequency, curCutoff sqlNullString
		var curBuffer sqlNullFloat
		var curAuto, curActive sqlNullString
		if err := tx.QueryRow(ctx, sel, req.SweepID).Scan(&curEntity, &curBank, &curAccount, &curType, &curParent, &curBuffer, &curFrequency, &curCutoff, &curAuto, &curActive); err != nil {
			api.RespondWithResult(w, false, "failed to fetch existing sweep config: "+err.Error())
			return
		}

		sets := []string{}
		args := []interface{}{}
		pos := 1

		addStrField := func(col, oldcol string, val interface{}, cur sqlNullString) {
			sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
			args = append(args, nullifyEmpty(fmt.Sprint(val)))
			args = append(args, cur.ValueOrZero())
			pos += 2
		}
		addFloatField := func(col, oldcol string, val interface{}, cur sqlNullFloat) {
			sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
			if val == nil {
				args = append(args, nil)
			} else {
				args = append(args, val)
			}
			args = append(args, cur.ValueOrZero())
			pos += 2
		}

		for k, v := range req.Fields {
			switch k {
			case "entity_name":
				addStrField("entity_name", "old_entity_name", v, curEntity)
			case "bank_name":
				addStrField("bank_name", "old_bank_name", v, curBank)
			case "bank_account":
				addStrField("bank_account", "old_bank_account", v, curAccount)
			case "sweep_type":
				addStrField("sweep_type", "old_sweep_type", v, curType)
			case "parent_account":
				addStrField("parent_account", "old_parent_account", v, curParent)
			case "buffer_amount":
				addFloatField("buffer_amount", "old_buffer_amount", v, curBuffer)
			case "frequency":
				addStrField("frequency", "old_frequency", v, curFrequency)
			case "cutoff_time":
				addStrField("cutoff_time", "old_cutoff_time", v, curCutoff)
			case "auto_sweep":
				addStrField("auto_sweep", "old_auto_sweep", v, curAuto)
			case "active_status":
				addStrField("active_status", "old_active_status", v, curActive)
			default:
				// ignore unknown
			}
		}

		if len(sets) == 0 {
			api.RespondWithResult(w, false, "no valid fields to update")
			return
		}

		q := "UPDATE mastersweepconfiguration SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE sweep_id=$%d", pos)
		args = append(args, req.SweepID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithResult(w, false, "failed to update sweep config: "+err.Error())
			return
		}

		auditQ := `INSERT INTO auditactionsweepconfiguration (sweep_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())`
		if _, err := tx.Exec(ctx, auditQ, req.SweepID, nullifyEmpty(req.Reason), requestedBy); err != nil {
			api.RespondWithResult(w, false, "failed to create audit action: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit: "+err.Error())
			return
		}
		committed = true

		api.RespondWithResult(w, true, req.SweepID)
	}
}

// GetSweepConfigurations returns mastersweepconfiguration rows with latest audit info
func GetSweepConfigurations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithResult(w, false, "Missing user_id in body")
			return
		}

		// validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// skip soft-deleted rows
		base := `SELECT sweep_id, entity_name, bank_name, bank_account, sweep_type, parent_account, buffer_amount, frequency, cutoff_time, auto_sweep, active_status, old_entity_name, old_bank_name, old_bank_account, old_sweep_type, old_parent_account, old_buffer_amount, old_frequency, old_cutoff_time, old_auto_sweep, old_active_status FROM mastersweepconfiguration WHERE is_deleted != TRUE ORDER BY created_at DESC, sweep_id`
		rows, err := pgxPool.Query(ctx, base)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var sweepID string
			var entity, bank, account, stype, parent, freq, cutoff, auto, active sqlNullString
			var buffer sqlNullFloat
			var oldEntity, oldBank, oldAccount, oldType, oldParent, oldFreq, oldCutoff, oldAuto, oldActive sqlNullString
			var oldBuffer sqlNullFloat
			if err := rows.Scan(&sweepID, &entity, &bank, &account, &stype, &parent, &buffer, &freq, &cutoff, &auto, &active, &oldEntity, &oldBank, &oldAccount, &oldType, &oldParent, &oldBuffer, &oldFreq, &oldCutoff, &oldAuto, &oldActive); err != nil {
				continue
			}

			auditLatest := `SELECT processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason FROM auditactionsweepconfiguration WHERE sweep_id = $1 ORDER BY requested_at DESC LIMIT 1`
			var processingStatusPtr, requestedByPtr, actionTypePtr, actionIDPtr, checkerByPtr, checkerCommentPtr, reasonPtr *string
			var requestedAtPtr, checkerAtPtr *time.Time
			_ = pgxPool.QueryRow(ctx, auditLatest, sweepID).Scan(&processingStatusPtr, &requestedByPtr, &requestedAtPtr, &actionTypePtr, &actionIDPtr, &checkerByPtr, &checkerAtPtr, &checkerCommentPtr, &reasonPtr)

			// Then fetch recent CREATE/EDIT/DELETE entries to build created/edited/deleted summary (use api.GetAuditInfo)
			auditDetailsQuery := `SELECT actiontype, requested_by, requested_at FROM auditactionsweepconfiguration WHERE sweep_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC`
			auditRows, auditErr := pgxPool.Query(ctx, auditDetailsQuery, sweepID)
			var createdBy, createdAt, editedBy, editedAt, deletedBy, deletedAt string
			if auditErr == nil {
				defer auditRows.Close()
				for auditRows.Next() {
					var atype string
					var rbyPtr *string
					var ratPtr *time.Time
					if err := auditRows.Scan(&atype, &rbyPtr, &ratPtr); err == nil {
						auditInfo := api.GetAuditInfo(atype, rbyPtr, ratPtr)
						if atype == "CREATE" && createdBy == "" {
							createdBy = auditInfo.CreatedBy
							createdAt = auditInfo.CreatedAt
						} else if atype == "EDIT" && editedBy == "" {
							editedBy = auditInfo.EditedBy
							editedAt = auditInfo.EditedAt
						} else if atype == "DELETE" && deletedBy == "" {
							deletedBy = auditInfo.DeletedBy
							deletedAt = auditInfo.DeletedAt
						}
					}
				}
			}

			m := map[string]interface{}{
				"sweep_id":           sweepID,
				"entity_name":        entity.ValueOrZero(),
				"bank_name":          bank.ValueOrZero(),
				"bank_account":       account.ValueOrZero(),
				"sweep_type":         stype.ValueOrZero(),
				"parent_account":     parent.ValueOrZero(),
				"buffer_amount":      buffer.ValueOrZero(),
				"frequency":          freq.ValueOrZero(),
				"cutoff_time":        cutoff.ValueOrZero(),
				"auto_sweep":         auto.ValueOrZero(),
				"active_status":      active.ValueOrZero(),
				"old_entity_name":    oldEntity.ValueOrZero(),
				"old_bank_name":      oldBank.ValueOrZero(),
				"old_bank_account":   oldAccount.ValueOrZero(),
				"old_sweep_type":     oldType.ValueOrZero(),
				"old_parent_account": oldParent.ValueOrZero(),
				"old_buffer_amount":  oldBuffer.ValueOrZero(),
				"old_frequency":      oldFreq.ValueOrZero(),
				"old_cutoff_time":    oldCutoff.ValueOrZero(),
				"old_auto_sweep":     oldAuto.ValueOrZero(),
				"old_active_status":  oldActive.ValueOrZero(),
				"processing_status": func() string {
					if processingStatusPtr != nil {
						return *processingStatusPtr
					}
					return ""
				}(),
				"action_type": func() string {
					if actionTypePtr != nil {
						return *actionTypePtr
					}
					return ""
				}(),
				"action_id": func() string {
					if actionIDPtr != nil {
						return *actionIDPtr
					}
					return ""
				}(),
				"checker_by": api.GetAuditInfo("", checkerByPtr, checkerAtPtr).CreatedBy,
				"checker_at": api.GetAuditInfo("", checkerByPtr, checkerAtPtr).CreatedAt,
				"checker_comment": func() string {
					if checkerCommentPtr != nil {
						return *checkerCommentPtr
					}
					return ""
				}(),
				"reason": func() string {
					if reasonPtr != nil {
						return *reasonPtr
					}
					return ""
				}(),
				"created_by": createdBy,
				"created_at": createdAt,
				"edited_by":  editedBy,
				"edited_at":  editedAt,
				"deleted_by": deletedBy,
				"deleted_at": deletedAt,
			}
			out = append(out, m)
		}
		if rows.Err() != nil {
			api.RespondWithResult(w, false, "DB rows error: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// BulkApproveSweepConfigurations approves pending audit actions for given sweep_ids.
func BulkApproveSweepConfigurations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string   `json:"user_id"`
			SweepIDs []string `json:"sweep_ids"`
			Comment  string   `json:"comment,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.SweepIDs) == 0 {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON)
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		sel := `SELECT DISTINCT ON (sweep_id) action_id, sweep_id, actiontype FROM auditactionsweepconfiguration WHERE sweep_id = ANY($1) ORDER BY sweep_id, requested_at DESC`
		rows, err := pgxPool.Query(ctx, sel, req.SweepIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch latest audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		deleteIDs := make([]string, 0)
		found := map[string]bool{}
		for rows.Next() {
			var actionID, sweepID, actionType string
			if err := rows.Scan(&actionID, &sweepID, &actionType); err != nil {
				continue
			}
			found[sweepID] = true
			actionIDs = append(actionIDs, actionID)
			if actionType == "DELETE" {
				deleteIDs = append(deleteIDs, sweepID)
			}
		}

		missing := []string{}
		for _, id := range req.SweepIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			api.RespondWithResult(w, false, fmt.Sprintf("missing audit entries for: %v", missing))
			return
		}

		upd := `UPDATE auditactionsweepconfiguration SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3)`
		if _, err := pgxPool.Exec(ctx, upd, checkerBy, nullifyEmpty(req.Comment), actionIDs); err != nil {
			api.RespondWithResult(w, false, "failed to approve actions: "+err.Error())
			return
		}

		deleted := []string{}
		if len(deleteIDs) > 0 {
			// perform soft-delete instead of hard delete
			updDel := `UPDATE mastersweepconfiguration SET is_deleted = TRUE, updated_at = now() WHERE sweep_id = ANY($1) RETURNING sweep_id`
			drows, derr := pgxPool.Query(ctx, updDel, deleteIDs)
			if derr == nil {
				defer drows.Close()
				for drows.Next() {
					var id string
					drows.Scan(&id)
					deleted = append(deleted, id)
				}
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"approved_count": len(actionIDs), "deleted": deleted})
	}
}

// BulkRejectSweepConfigurations rejects latest audit actions for given sweep_ids.
func BulkRejectSweepConfigurations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string   `json:"user_id"`
			SweepIDs []string `json:"sweep_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.SweepIDs) == 0 {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON)
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		sel := `SELECT DISTINCT ON (sweep_id) action_id, sweep_id FROM auditactionsweepconfiguration WHERE sweep_id = ANY($1) ORDER BY sweep_id, requested_at DESC`
		rows, err := pgxPool.Query(ctx, sel, req.SweepIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch latest audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		found := map[string]bool{}
		for rows.Next() {
			var actionID, sweepID string
			if err := rows.Scan(&actionID, &sweepID); err != nil {
				continue
			}
			found[sweepID] = true
			actionIDs = append(actionIDs, actionID)
		}
		missing := []string{}
		for _, id := range req.SweepIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			api.RespondWithResult(w, false, fmt.Sprintf("missing audit entries for: %v", missing))
			return
		}

		upd := `UPDATE auditactionsweepconfiguration SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3)`
		if _, err := pgxPool.Exec(ctx, upd, checkerBy, nullifyEmpty(req.Comment), actionIDs); err != nil {
			api.RespondWithResult(w, false, "failed to reject actions: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"rejected_count": len(actionIDs)})
	}
}

// BulkRequestDeleteSweepConfigurations inserts DELETE audit actions (PENDING_DELETE_APPROVAL) for sweep configs
func BulkRequestDeleteSweepConfigurations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string   `json:"user_id"`
			SweepIDs []string `json:"sweep_ids"`
			Reason   string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.SweepIDs) == 0 {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON)
			return
		}
		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "failed to begin tx: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		ins := `INSERT INTO auditactionsweepconfiguration (sweep_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`
		for _, id := range req.SweepIDs {
			if _, err := tx.Exec(ctx, ins, id, nullifyEmpty(req.Reason), requestedBy); err != nil {
				api.RespondWithResult(w, false, "failed to create delete audit: "+err.Error())
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit: "+err.Error())
			return
		}
		committed = true

		api.RespondWithResult(w, true, fmt.Sprintf("created %d delete requests", len(req.SweepIDs)))
	}
}

// helpers
func nullifyEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
func nullifyFloat(f *float64) interface{} {
	if f == nil {
		return nil
	}
	return *f
}
func nullifyBool(b *bool) interface{} {
	if b == nil {
		return nil
	}
	return *b
}

type sqlNullString struct {
	Valid bool
	S     string
}

func (n *sqlNullString) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		n.S = ""
		return nil
	}
	switch t := v.(type) {
	case string:
		n.Valid = true
		n.S = t
	case []byte:
		n.Valid = true
		n.S = string(t)
	default:
		n.Valid = true
		n.S = fmt.Sprint(v)
	}
	return nil
}
func (n sqlNullString) ValueOrZero() interface{} {
	if n.Valid {
		return n.S
	}
	return ""
}

type sqlNullFloat struct {
	Valid bool
	F     float64
}

func (n *sqlNullFloat) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		n.F = 0
		return nil
	}
	switch t := v.(type) {
	case float64:
		n.Valid = true
		n.F = t
	case int64:
		n.Valid = true
		n.F = float64(t)
	case []byte:
		s := string(t)
		if s == "" {
			n.Valid = false
			n.F = 0
			return nil
		}
		var err error
		n.F, err = strconv.ParseFloat(s, 64)
		if err != nil {
			n.Valid = false
			n.F = 0
			return nil
		}
		n.Valid = true
	case string:
		if t == "" {
			n.Valid = false
			n.F = 0
			return nil
		}
		var err error
		n.F, err = strconv.ParseFloat(t, 64)
		if err != nil {
			n.Valid = false
			n.F = 0
			return nil
		}
		n.Valid = true
	default:
		n.Valid = true
		n.F = 0
	}
	return nil
}
func (n sqlNullFloat) ValueOrZero() interface{} {
	if n.Valid {
		return n.F
	}
	return nil
}
