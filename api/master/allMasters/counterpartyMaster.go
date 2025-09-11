package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Minimal request shape for counterparty create/sync
type CounterpartyRequest struct {
	InputMethod             string `json:"input_method"`
	SystemCounterpartyID    string `json:"system_counterparty_id"`
	CounterpartyName        string `json:"counterparty_name"`
	CounterpartyCode        string `json:"counterparty_code"`
	LegalName               string `json:"legal_name"`
	CounterpartyType        string `json:"counterparty_type"`
	TaxID                   string `json:"tax_id"`
	Address                 string `json:"address"`
	ErpRefID                string `json:"erp_ref_id"`
	DefaultCashflowCategory string `json:"default_cashflow_category"`
	DefaultPaymentTerms     string `json:"default_payment_terms"`
	InternalRiskRating      string `json:"internal_risk_rating"`
	TreasuryRM              string `json:"treasury_rm"`
	Status                  string `json:"status"`
}

// CreateCounterparties inserts rows into mastercounterparty and creates audit entries
func CreateCounterparties(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string                `json:"user_id"`
			Rows   []CounterpartyRequest `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// validate session and get created_by
		createdBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				createdBy = s.Email
				break
			}
		}
		if createdBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := r.Context()
		created := make([]map[string]interface{}, 0)

		for _, rrow := range req.Rows {
			if rrow.SystemCounterpartyID == "" || rrow.CounterpartyName == "" || rrow.CounterpartyCode == "" {
				created = append(created, map[string]interface{}{"success": false, "error": "missing required fields", "counterparty_name": rrow.CounterpartyName})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				created = append(created, map[string]interface{}{"success": false, "error": "failed to start tx: " + err.Error()})
				continue
			}

			var id string
			q := `INSERT INTO mastercounterparty (input_method, system_counterparty_id, counterparty_name, counterparty_code, legal_name, counterparty_type, tax_id, address, erp_ref_id, default_cashflow_category, default_payment_terms, internal_risk_rating, treasury_rm, status)
				  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING counterparty_id`
			if err := tx.QueryRow(ctx, q, rrow.InputMethod, rrow.SystemCounterpartyID, rrow.CounterpartyName, rrow.CounterpartyCode, rrow.LegalName, rrow.CounterpartyType, rrow.TaxID, rrow.Address, rrow.ErpRefID, rrow.DefaultCashflowCategory, rrow.DefaultPaymentTerms, rrow.InternalRiskRating, rrow.TreasuryRM, rrow.Status).Scan(&id); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": err.Error(), "counterparty_name": rrow.CounterpartyName})
				continue
			}

			auditQ := `INSERT INTO auditactioncounterparty (counterparty_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, auditQ, id, nil, createdBy); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": "audit insert failed: " + err.Error(), "id": id})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": "commit failed: " + err.Error(), "id": id})
				continue
			}
			created = append(created, map[string]interface{}{"success": true, "counterparty_id": id, "counterparty_name": rrow.CounterpartyName})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": created})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := r.Context()

		mainQ := `
			WITH latest AS (
				SELECT DISTINCT ON (counterparty_id) counterparty_id, processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason
				FROM auditactioncounterparty
				ORDER BY counterparty_id, requested_at DESC
			)
			SELECT m.counterparty_id, m.input_method, m.system_counterparty_id, m.old_system_counterparty_id, m.counterparty_name, m.old_counterparty_name, m.counterparty_code, m.old_counterparty_code, m.legal_name, m.old_legal_name, m.counterparty_type, m.old_counterparty_type, m.tax_id, m.old_tax_id, m.address, m.old_address, m.erp_ref_id, m.old_erp_ref_id, m.default_cashflow_category, m.old_default_cashflow_category, m.default_payment_terms, m.old_default_payment_terms, m.internal_risk_rating, m.old_internal_risk_rating, m.treasury_rm, m.old_treasury_rm, m.status, m.old_status,
				   l.processing_status, l.requested_by, l.requested_at, l.actiontype, l.action_id, l.checker_by, l.checker_at, l.checker_comment, l.reason
			FROM mastercounterparty m
			LEFT JOIN latest l ON l.counterparty_id = m.counterparty_id
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
				sysID            interface{}
				oldSysID         interface{}
				name             interface{}
				oldName          interface{}
				code             interface{}
				oldCode          interface{}
				legal            interface{}
				oldLegal         interface{}
				ctype            interface{}
				oldCtype         interface{}
				tax              interface{}
				oldTax           interface{}
				addr             interface{}
				oldAddr          interface{}
				erp              interface{}
				oldErp           interface{}
				defCat           interface{}
				oldDefCat        interface{}
				defTerms         interface{}
				oldDefTerms      interface{}
				risk             interface{}
				oldRisk          interface{}
				rm               interface{}
				oldRm            interface{}
				status           interface{}
				oldStatus        interface{}
				processingStatus interface{}
				requestedBy      interface{}
				requestedAt      interface{}
				actionType       interface{}
				actionID         string
				checkerBy        interface{}
				checkerAt        interface{}
				checkerComment   interface{}
				reason           interface{}
			)

			if err := rows.Scan(&id, &inputMethod, &sysID, &oldSysID, &name, &oldName, &code, &oldCode, &legal, &oldLegal, &ctype, &oldCtype, &tax, &oldTax, &addr, &oldAddr, &erp, &oldErp, &defCat, &oldDefCat, &defTerms, &oldDefTerms, &risk, &oldRisk, &rm, &oldRm, &status, &oldStatus, &processingStatus, &requestedBy, &requestedAt, &actionType, &actionID, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
				continue
			}

			order = append(order, id)
			outMap[id] = map[string]interface{}{
				"counterparty_id":               id,
				"input_method":                  ifaceToString(inputMethod),
				"system_counterparty_id":        ifaceToString(sysID),
				"old_system_counterparty_id":    ifaceToString(oldSysID),
				"counterparty_name":             ifaceToString(name),
				"old_counterparty_name":         ifaceToString(oldName),
				"counterparty_code":             ifaceToString(code),
				"old_counterparty_code":         ifaceToString(oldCode),
				"legal_name":                    ifaceToString(legal),
				"old_legal_name":                ifaceToString(oldLegal),
				"counterparty_type":             ifaceToString(ctype),
				"old_counterparty_type":         ifaceToString(oldCtype),
				"tax_id":                        ifaceToString(tax),
				"old_tax_id":                    ifaceToString(oldTax),
				"address":                       ifaceToString(addr),
				"old_address":                   ifaceToString(oldAddr),
				"erp_ref_id":                    ifaceToString(erp),
				"old_erp_ref_id":                ifaceToString(oldErp),
				"default_cashflow_category":     ifaceToString(defCat),
				"old_default_cashflow_category": ifaceToString(oldDefCat),
				"default_payment_terms":         ifaceToString(defTerms),
				"old_default_payment_terms":     ifaceToString(oldDefTerms),
				"internal_risk_rating":          ifaceToString(risk),
				"old_internal_risk_rating":      ifaceToString(oldRisk),
				"treasury_rm":                   ifaceToString(rm),
				"old_treasury_rm":               ifaceToString(oldRm),
				"status":                        ifaceToString(status),
				"old_status":                    ifaceToString(oldStatus),
				"processing_status":             ifaceToString(processingStatus),
				"requested_by":                  ifaceToString(requestedBy),
				"requested_at":                  ifaceToTimeString(requestedAt),
				"action_type":                   ifaceToString(actionType),
				"action_id":                     actionID,
				"checker_by":                    ifaceToString(checkerBy),
				"checker_at":                    ifaceToTimeString(checkerAt),
				"checker_comment":               ifaceToString(checkerComment),
				"reason":                        ifaceToString(reason),
				"created_by":                    "", "created_at": "", "edited_by": "", "edited_at": "", "deleted_by": "", "deleted_at": "",
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

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
	}
}

// GetApprovedActiveCounterparties returns minimal rows where latest audit is APPROVED and master status is Active
func GetApprovedActiveCounterparties(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		updatedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				updatedBy = s.Email
				break
			}
		}
		if updatedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := r.Context()
		results := []map[string]interface{}{}
		for _, row := range req.Rows {
			if row.CounterpartyID == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "missing counterparty_id"})
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "begin failed: " + err.Error()})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
				}()
				sel := `SELECT system_counterparty_id, counterparty_name, counterparty_code, legal_name, counterparty_type, tax_id, address, default_cashflow_category, default_payment_terms, internal_risk_rating, treasury_rm, status FROM mastercounterparty WHERE counterparty_id=$1 FOR UPDATE`
				var existingSysID, existingName, existingCode, existingLegal, existingType, existingTax, existingAddr, existingDefCat, existingDefTerms, existingRisk, existingRM, existingStatus interface{}
				if err := tx.QueryRow(ctx, sel, row.CounterpartyID).Scan(&existingSysID, &existingName, &existingCode, &existingLegal, &existingType, &existingTax, &existingAddr, &existingDefCat, &existingDefTerms, &existingRisk, &existingRM, &existingStatus); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "fetch failed: " + err.Error(), "counterparty_id": row.CounterpartyID})
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
					case "system_counterparty_id":
						sets = append(sets, fmt.Sprintf("system_counterparty_id=$%d, old_system_counterparty_id=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSysID))
						pos += 2
					case "legal_name":
						sets = append(sets, fmt.Sprintf("legal_name=$%d, old_legal_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingLegal))
						pos += 2
					case "tax_id":
						sets = append(sets, fmt.Sprintf("tax_id=$%d, old_tax_id=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTax))
						pos += 2
					case "address":
						sets = append(sets, fmt.Sprintf("address=$%d, old_address=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingAddr))
						pos += 2
					case "counterparty_code":
						sets = append(sets, fmt.Sprintf("counterparty_code=$%d, old_counterparty_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCode))
						pos += 2
					case "counterparty_type":
						sets = append(sets, fmt.Sprintf("counterparty_type=$%d, old_counterparty_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingType))
						pos += 2
					case "default_cashflow_category":
						vStr := strings.TrimSpace(fmt.Sprint(v))
						if vStr == "" {
							sets = append(sets, fmt.Sprintf("default_cashflow_category=$%d, old_default_cashflow_category=$%d", pos, pos+1))
							args = append(args, nil, ifaceToString(existingDefCat))
							pos += 2
						} else {
							// validate category exists in mastercashflowcategory
							var exists bool
							if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM mastercashflowcategory WHERE category_name=$1)", vStr).Scan(&exists); err != nil {
								results = append(results, map[string]interface{}{"success": false, "error": "failed to validate default_cashflow_category: " + err.Error(), "counterparty_id": row.CounterpartyID})
								return
							}
							if !exists {
								results = append(results, map[string]interface{}{"success": false, "error": "invalid default_cashflow_category: " + vStr, "counterparty_id": row.CounterpartyID})
								return
							}
							sets = append(sets, fmt.Sprintf("default_cashflow_category=$%d, old_default_cashflow_category=$%d", pos, pos+1))
							args = append(args, vStr, ifaceToString(existingDefCat))
							pos += 2
						}
					case "default_payment_terms":
						sets = append(sets, fmt.Sprintf("default_payment_terms=$%d, old_default_payment_terms=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingDefTerms))
						pos += 2
					case "internal_risk_rating":
						sets = append(sets, fmt.Sprintf("internal_risk_rating=$%d, old_internal_risk_rating=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingRisk))
						pos += 2
					case "treasury_rm":
						sets = append(sets, fmt.Sprintf("treasury_rm=$%d, old_treasury_rm=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingRM))
						pos += 2
					case "status":
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingStatus))
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
						results = append(results, map[string]interface{}{"success": false, "error": "update failed: " + err.Error(), "counterparty_id": row.CounterpartyID})
						return
					}
				}

				auditQ := `INSERT INTO auditactioncounterparty (counterparty_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL', $2, $3, now())`
				if _, err := tx.Exec(ctx, auditQ, updatedID, row.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "audit failed: " + err.Error(), "counterparty_id": updatedID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "commit failed: " + err.Error(), "counterparty_id": updatedID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{"success": true, "counterparty_id": updatedID})
			}()
		}

		overall := true
		for _, r := range results {
			if ok, exists := r["success"]; exists {
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": overall, "rows": results})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == body.UserID {
				requestedBy = s.Email
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		if len(body.CounterpartyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_ids required")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to start transaction: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		committed = true

		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "counterparty_ids": body.CounterpartyIDs})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to start transaction: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		committed = true
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to start transaction: "+err.Error())
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
			delQ := `DELETE FROM mastercounterparty WHERE counterparty_id = ANY($1)`
			if _, err := tx.Exec(ctx, delQ, deleteIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to delete master rows: "+err.Error())
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		committed = true
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}

// UploadCounterparty handles staging and mapping for mastercounterparty
func UploadCounterparty(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := ""
		if r.Header.Get("Content-Type") == "application/json" {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in body")
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue("user_id")
			if userID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in form")
				return
			}
		}
		userName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "User not found in active sessions")
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse multipart form")
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No files uploaded")
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
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction: "+err.Error())
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
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("No mapping for source column: %s", h))
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
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			committed = true
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "batch_ids": batchIDs})
	}
}

// (no nullableUUID helper required here)
