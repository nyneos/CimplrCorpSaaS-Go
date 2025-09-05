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

// Minimal request shape
type CostProfitCenterRequest struct {
	CentreCode string `json:"centre_code"`
	CentreName string `json:"centre_name"`
	CentreType string `json:"centre_type"`
	ParentCode string `json:"parent_centre_code"`
	EntityCode string `json:"entity_code"`
	Status     string `json:"status"`
	Source     string `json:"source"`
	ErpRef     string `json:"erp_ref"`
	// optional: if caller wants to override computed values
	CentreLevel      int  `json:"centre_level,omitempty"`
	IsTopLevelCentre bool `json:"is_top_level_centre,omitempty"`
}

// CreateAndSyncCostProfitCenters creates centres and synchronously creates parent-child relationships
func CreateAndSyncCostProfitCenters(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string                    `json:"user_id"`
			Rows   []CostProfitCenterRequest `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
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

		ctx := context.Background()
		created := make([]map[string]interface{}, 0)

		for _, rrow := range req.Rows {
			if strings.TrimSpace(rrow.CentreCode) == "" || strings.TrimSpace(rrow.CentreName) == "" || strings.TrimSpace(rrow.CentreType) == "" {
				created = append(created, map[string]interface{}{"success": false, "error": "missing required fields", "centre_code": rrow.CentreCode})
				continue
			}

			// compute parent id and level
			var parentID interface{}
			centreLevel := rrow.CentreLevel
			isTop := rrow.IsTopLevelCentre
			if strings.TrimSpace(rrow.ParentCode) != "" {
				var pid string
				var plevel int
				if err := pgxPool.QueryRow(ctx, `SELECT centre_id, centre_level FROM mastercostprofitcenter WHERE centre_code=$1`, rrow.ParentCode).Scan(&pid, &plevel); err == nil {
					parentID = pid
					centreLevel = plevel + 1
					isTop = false
				}
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				created = append(created, map[string]interface{}{"success": false, "error": "begin failed: " + err.Error()})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
				}()
				var id string
				ins := `INSERT INTO mastercostprofitcenter (centre_code, centre_name, centre_type, parent_centre_id, entity_name, status, source, erp_ref, centre_level, is_top_level_centre) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING centre_id`
				if err := tx.QueryRow(ctx, ins, rrow.CentreCode, rrow.CentreName, rrow.CentreType, parentID, rrow.EntityCode, rrow.Status, rrow.Source, rrow.ErpRef, centreLevel, isTop).Scan(&id); err != nil {
					created = append(created, map[string]interface{}{"success": false, "error": err.Error(), "centre_code": rrow.CentreCode})
					return
				}

				// create relationship row if parent provided
				if parentID != nil {
					relQ := `INSERT INTO costprofitcenterrelationships (parent_centre_id, child_centre_id) SELECT $1, $2 WHERE NOT EXISTS (SELECT 1 FROM costprofitcenterrelationships WHERE parent_centre_id=$1 AND child_centre_id=$2)`
					if _, err := tx.Exec(ctx, relQ, parentID, id); err != nil {
						created = append(created, map[string]interface{}{"success": false, "error": "relationship insert failed: " + err.Error(), "centre_code": rrow.CentreCode})
						return
					}
				}

				auditQ := `INSERT INTO auditactioncostprofitcenter (centre_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
				if _, err := tx.Exec(ctx, auditQ, id, nil, createdBy); err != nil {
					created = append(created, map[string]interface{}{"success": false, "error": "audit insert failed: " + err.Error(), "centre_id": id})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					created = append(created, map[string]interface{}{"success": false, "error": "commit failed: " + err.Error(), "centre_id": id})
					return
				}
				committed = true
				created = append(created, map[string]interface{}{"success": true, "centre_id": id, "centre_code": rrow.CentreCode, "centre_level": centreLevel, "is_top_level_centre": isTop})
			}()
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(created)
	}
}

// UpdateAndSyncCostProfitCenters updates centres and synchronously updates relationships when parent changes
func UpdateAndSyncCostProfitCenters(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				CentreID string                 `json:"centre_id"`
				Fields   map[string]interface{} `json:"fields"`
				Reason   string                 `json:"reason"`
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

		ctx := context.Background()
		results := []map[string]interface{}{}
		for _, row := range req.Rows {
			if strings.TrimSpace(row.CentreID) == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "missing centre_id"})
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

				// fetch existing full row for old_* population and locking
				var existingCode, existingName, existingType, existingParentID, existingEntity, existingStatus, existingSource, existingErp interface{}
				var existingLevel interface{}
				var existingIsTop interface{}
				qSel := `SELECT centre_code, centre_name, centre_type, parent_centre_id, entity_name, status, source, erp_ref, centre_level, is_top_level_centre FROM mastercostprofitcenter WHERE centre_id = $1 FOR UPDATE`
				if err := tx.QueryRow(ctx, qSel, row.CentreID).Scan(&existingCode, &existingName, &existingType, &existingParentID, &existingEntity, &existingStatus, &existingSource, &existingErp, &existingLevel, &existingIsTop); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "fetch failed: " + err.Error(), "centre_id": row.CentreID})
					return
				}

				var sets []string
				var args []interface{}
				pos := 1
				var newParentID interface{}
				var computedLevel interface{}
				var computedIsTop interface{}

				for k, v := range row.Fields {
					switch k {
					case "centre_code":
						sets = append(sets, fmt.Sprintf("centre_code=$%d, old_centre_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCode))
						pos += 2
					case "centre_name":
						sets = append(sets, fmt.Sprintf("centre_name=$%d, old_centre_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingName))
						pos += 2
					case "centre_type":
						sets = append(sets, fmt.Sprintf("centre_type=$%d, old_centre_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingType))
						pos += 2
					case "entity_name", "entity_code":
						// both possible incoming names - map to entity_name
						sets = append(sets, fmt.Sprintf("entity_name=$%d, old_entity_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingEntity))
						pos += 2
					case "status":
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingStatus))
						pos += 2
					case "source":
						sets = append(sets, fmt.Sprintf("source=$%d, old_source=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSource))
						pos += 2
					case "erp_ref":
						sets = append(sets, fmt.Sprintf("erp_ref=$%d, old_erp_ref=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingErp))
						pos += 2
					case "centre_level":
						// numeric provided directly
						sets = append(sets, fmt.Sprintf("centre_level=$%d, old_centre_level=$%d", pos, pos+1))
						args = append(args, v, existingLevel)
						pos += 2
					case "is_top_level_centre":
						sets = append(sets, fmt.Sprintf("is_top_level_centre=$%d", pos))
						args = append(args, v)
						pos += 1
					case "parent_centre_code":
						pcode := strings.TrimSpace(fmt.Sprint(v))
						if pcode == "" {
							// removing parent
							newParentID = nil
							computedLevel = 0
							computedIsTop = true
						} else {
							var pid string
							var plevel int
							if err := pgxPool.QueryRow(ctx, `SELECT centre_id, centre_level FROM mastercostprofitcenter WHERE centre_code=$1`, pcode).Scan(&pid, &plevel); err != nil {
								results = append(results, map[string]interface{}{"success": false, "error": "parent centre not found: " + pcode, "centre_id": row.CentreID})
								return
							}
							newParentID = pid
							computedLevel = plevel + 1
							computedIsTop = false
						}
						// set parent and old_parent and update level fields
						sets = append(sets, fmt.Sprintf("parent_centre_id=$%d, old_parent_centre_id=$%d, centre_level=$%d, old_centre_level=$%d, is_top_level_centre=$%d", pos, pos+1, pos+2, pos+3, pos+4))
						args = append(args, newParentID, ifaceToString(existingParentID), computedLevel, existingLevel, computedIsTop)
						pos += 5
					default:
						// ignore unknown fields
					}
				}

				updatedID := row.CentreID
				if len(sets) > 0 {
					q := "UPDATE mastercostprofitcenter SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE centre_id=$%d RETURNING centre_id", pos)
					args = append(args, row.CentreID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedID); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "update failed: " + err.Error(), "centre_id": row.CentreID})
						return
					}
				}

				// sync relationships when parent changed
				// compare existingParent and newParentID
				parentChanged := false
				if newParentID != nil {
					if ifaceToString(existingParentID) != fmt.Sprint(newParentID) {
						parentChanged = true
					}
				} else {
					if existingParentID != nil {
						parentChanged = true
					}
				}

				if parentChanged {
					// remove any relationship entries where this child has a different parent
					delQ := `DELETE FROM costprofitcenterrelationships WHERE child_centre_id=$1 AND parent_centre_id IS DISTINCT FROM $2`
					if _, err := tx.Exec(ctx, delQ, updatedID, newParentID); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "failed to remove old relationships: " + err.Error(), "centre_id": updatedID})
						return
					}
					// insert new relationship if parent exists
					if newParentID != nil {
						relQ := `INSERT INTO costprofitcenterrelationships (parent_centre_id, child_centre_id) SELECT $1, $2 WHERE NOT EXISTS (SELECT 1 FROM costprofitcenterrelationships WHERE parent_centre_id=$1 AND child_centre_id=$2)`
						if _, err := tx.Exec(ctx, relQ, newParentID, updatedID); err != nil {
							results = append(results, map[string]interface{}{"success": false, "error": "relationship insert failed: " + err.Error(), "centre_id": updatedID})
							return
						}
					}
				}

				auditQ := `INSERT INTO auditactioncostprofitcenter (centre_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL', $2, $3, now())`
				if _, err := tx.Exec(ctx, auditQ, updatedID, row.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "audit failed: " + err.Error(), "centre_id": updatedID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "commit failed: " + err.Error(), "centre_id": updatedID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{"success": true, "centre_id": updatedID})

			}()
		}
		overall := true
		for _, r := range results {
			if ok, exists := r["success"]; !exists || ok != true {
				overall = false
				break
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": overall, "rows": results})
	}
}

// UploadAndSyncCostProfitCenters uploads file(s), inserts masters and creates relationships based on parent_centre_code
func UploadAndSyncCostProfitCenters(pgxPool *pgxpool.Pool) http.HandlerFunc {
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

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"input_costprofitcenter"}, columns, pgx.CopyFromRows(copyRows)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}

			// read mapping
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_costprofitcenter`)
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
				if strings.ToLower(tgt) == "parent_centre_id" {
					selectExprs = append(selectExprs, "NULL AS parent_centre_id")
				} else {
					selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", src, tgt))
				}
			}
			srcColsStr := strings.Join(selectExprs, ", ")

			insertSQL := fmt.Sprintf(`INSERT INTO mastercostprofitcenter (%s) SELECT %s FROM input_costprofitcenter s WHERE s.upload_batch_id = $1 RETURNING centre_id, centre_code`, tgtColsStr, srcColsStr)
			rows2, err := tx.Query(ctx, insertSQL, batchID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Final insert error: "+err.Error())
				return
			}
			var newIDs []string
			for rows2.Next() {
				var id, code string
				if err := rows2.Scan(&id, &code); err == nil {
					newIDs = append(newIDs, id)
				}
			}
			rows2.Close()

			// create relationships from input rows where parent_centre_id column exists
			relRows, err := tx.Query(ctx, `SELECT centre_code, parent_centre_id FROM input_costprofitcenter WHERE upload_batch_id=$1`, batchID)
			if err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read relationship inputs: "+err.Error())
				return
			}
			defer relRows.Close()
			for relRows.Next() {
				var childCode, parentCode interface{}
				if err := relRows.Scan(&childCode, &parentCode); err == nil {
					cc := strings.TrimSpace(ifaceToString(childCode))
					pc := strings.TrimSpace(ifaceToString(parentCode))
					if cc == "" || pc == "" {
						continue
					}
					// find child id and parent id by centre_code / parent centre identifier
					var childID, parentID string
					if err := pgxPool.QueryRow(ctx, `SELECT centre_id FROM mastercostprofitcenter WHERE centre_code=$1`, cc).Scan(&childID); err != nil {
						// skip if child not found
						continue
					}
					// if parent value looks like a UUID, try to resolve directly; otherwise try as centre_code
					if _, perr := uuid.Parse(pc); perr == nil {
						if err := pgxPool.QueryRow(ctx, `SELECT centre_id FROM mastercostprofitcenter WHERE centre_id=$1`, pc).Scan(&parentID); err != nil {
							continue
						}
					} else {
						if err := pgxPool.QueryRow(ctx, `SELECT centre_id FROM mastercostprofitcenter WHERE centre_code=$1`, pc).Scan(&parentID); err != nil {
							continue
						}
					}
					relQ := `INSERT INTO costprofitcenterrelationships (parent_centre_id, child_centre_id) SELECT $1, $2 WHERE NOT EXISTS (SELECT 1 FROM costprofitcenterrelationships WHERE parent_centre_id=$1 AND child_centre_id=$2)`
					if _, err := tx.Exec(ctx, relQ, parentID, childID); err != nil {
						tx.Rollback(ctx)
						api.RespondWithError(w, http.StatusInternalServerError, "relationship insert failed: "+err.Error())
						return
					}
				}
			}

			if len(newIDs) > 0 {
				auditSQL := `INSERT INTO auditactioncostprofitcenter (centre_id, actiontype, processing_status, reason, requested_by, requested_at) SELECT centre_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now() FROM mastercostprofitcenter WHERE centre_id = ANY($2)`
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

// GetApprovedActiveCostProfitCenters returns minimal rows where latest audit is APPROVED and status is ACTIVE
func GetApprovedActiveCostProfitCenters(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
                SELECT DISTINCT ON (centre_id) centre_id, processing_status
                FROM auditactioncostprofitcenter
                ORDER BY centre_id, requested_at DESC
            )
            SELECT m.centre_id, m.centre_code, m.centre_name, m.centre_type
            FROM mastercostprofitcenter m
            JOIN latest l ON l.centre_id = m.centre_id
            WHERE UPPER(l.processing_status) = 'APPROVED' AND UPPER(m.status) = 'ACTIVE' AND is_deleted = false
        `
		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		out := []map[string]interface{}{}
		for rows.Next() {
			var id string
			var code, name, typ interface{}
			if err := rows.Scan(&id, &code, &name, &typ); err == nil {
				out = append(out, map[string]interface{}{"centre_id": id, "centre_code": ifaceToString(code), "centre_name": ifaceToString(name), "centre_type": ifaceToString(typ)})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
	}
}

// GetCostProfitCenterHierarchy returns full rows with latest audit info and bulk history and relationships
func GetCostProfitCenterHierarchy(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
		query := `
            SELECT m.centre_id, m.centre_code, m.centre_name, m.centre_type, m.parent_centre_id, m.entity_name, m.status,
                   m.old_centre_code, m.old_centre_name, m.old_centre_type, m.old_parent_centre_id, m.old_entity_name, m.old_status,
                   m.centre_level, m.old_centre_level, m.is_top_level_centre, m.is_deleted,
                   a.processing_status, a.requested_by, a.requested_at, a.actiontype, a.action_id,
                   a.checker_by, a.checker_at, a.checker_comment, a.reason
            FROM mastercostprofitcenter m
            LEFT JOIN LATERAL (
                SELECT processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason
                FROM auditactioncostprofitcenter a
                WHERE a.centre_id = m.centre_id
                ORDER BY requested_at DESC
                LIMIT 1
            ) a ON TRUE
        `
		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		// build entityMap like cashflow implementation: id, name, data{...}, children[]
		entityMap := map[string]map[string]interface{}{}
		centreIDs := []string{}
		hideIds := map[string]bool{}

		for rows.Next() {
			var (
				centreID, actionIDI string
				centreCodeI, centreNameI, centreTypeI, parentCentreIDI, entityNameI, statusI,
				oldCodeI, oldNameI, oldTypeI, oldParentI, oldEntityI, oldStatusI,
				centreLevelI, oldCentreLevelI, procStatusI, requestedByI, requestedAtI, actionTypeI,
				checkerByI, checkerAtI, checkerCommentI, reasonI interface{}
				isTopLevel, isDeleted bool
			)

			if err := rows.Scan(&centreID, &centreCodeI, &centreNameI, &centreTypeI, &parentCentreIDI, &entityNameI, &statusI,
				&oldCodeI, &oldNameI, &oldTypeI, &oldParentI, &oldEntityI, &oldStatusI,
				&centreLevelI, &oldCentreLevelI, &isTopLevel, &isDeleted,
				&procStatusI, &requestedByI, &requestedAtI, &actionTypeI, &actionIDI,
				&checkerByI, &checkerAtI, &checkerCommentI, &reasonI); err != nil {
				continue
			}

			entityMap[centreID] = map[string]interface{}{
				"id":   centreID,
				"name": ifaceToString(centreNameI),
				"data": map[string]interface{}{
					"centre_id":            centreID,
					"centre_code":          ifaceToString(centreCodeI),
					"centre_name":          ifaceToString(centreNameI),
					"centre_type":          ifaceToString(centreTypeI),
					"parent_centre_id":     ifaceToString(parentCentreIDI),
					"entity_name":          ifaceToString(entityNameI),
					"status":               ifaceToString(statusI),
					"old_centre_code":      ifaceToString(oldCodeI),
					"old_centre_name":      ifaceToString(oldNameI),
					"old_centre_type":      ifaceToString(oldTypeI),
					"old_parent_centre_id": ifaceToString(oldParentI),
					"old_entity_name":      ifaceToString(oldEntityI),
					"old_status":           ifaceToString(oldStatusI),
					"centre_level":         ifaceToInt(centreLevelI),
					"old_centre_level":     ifaceToInt(oldCentreLevelI),
					"is_top_level_centre":  isTopLevel,
					"is_deleted":           isDeleted,
					"processing_status":    ifaceToString(procStatusI),
					"action_type":          ifaceToString(actionTypeI),
					"action_id":            actionIDI,
					"checker_by":           ifaceToString(checkerByI),
					"checker_at":           ifaceToTimeString(checkerAtI),
					"checker_comment":      ifaceToString(checkerCommentI),
					"reason":               ifaceToString(reasonI),
					"requested_by":         ifaceToString(requestedByI),
					"requested_at":         ifaceToTimeString(requestedAtI),
				},
				"children": []interface{}{},
			}

			centreIDs = append(centreIDs, centreID)
			if isDeleted && strings.ToUpper(ifaceToString(procStatusI)) == "APPROVED" {
				hideIds[centreID] = true
			}
		}

		// bulk fetch CREATE/EDIT/DELETE audit history and merge
		if len(centreIDs) > 0 {
			auditQ := `SELECT centre_id, actiontype, requested_by, requested_at FROM auditactioncostprofitcenter WHERE centre_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC`
			arows, err := pgxPool.Query(ctx, auditQ, centreIDs)
			if err == nil {
				defer arows.Close()
				auditMap := make(map[string]map[string]string)
				for arows.Next() {
					var cid, atype string
					var rby, rat interface{}
					if err := arows.Scan(&cid, &atype, &rby, &rat); err == nil {
						if auditMap[cid] == nil {
							auditMap[cid] = map[string]string{}
						}
						switch atype {
						case "CREATE":
							if auditMap[cid]["created_by"] == "" {
								auditMap[cid]["created_by"] = ifaceToString(rby)
								auditMap[cid]["created_at"] = ifaceToTimeString(rat)
							}
						case "EDIT":
							if auditMap[cid]["edited_by"] == "" {
								auditMap[cid]["edited_by"] = ifaceToString(rby)
								auditMap[cid]["edited_at"] = ifaceToTimeString(rat)
							}
						case "DELETE":
							if auditMap[cid]["deleted_by"] == "" {
								auditMap[cid]["deleted_by"] = ifaceToString(rby)
								auditMap[cid]["deleted_at"] = ifaceToTimeString(rat)
							}
						}
					}
				}
				for cid, info := range auditMap {
					if ent, ok := entityMap[cid]; ok {
						data := ent["data"].(map[string]interface{})
						for k, v := range info {
							data[k] = v
						}
					}
				}
			}
		}

		// fetch relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_centre_id, child_centre_id FROM costprofitcenterrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var p, c string
			if err := relRows.Scan(&p, &c); err == nil {
				parentMap[p] = append(parentMap[p], c)
			}
		}

		// hide deleted+approved nodes and their descendants
		if len(hideIds) > 0 {
			getAllDescendants := func(start []string) []string {
				all := map[string]bool{}
				queue := append([]string{}, start...)
				for _, id := range start {
					all[id] = true
				}
				for len(queue) > 0 {
					cur := queue[0]
					queue = queue[1:]
					for _, child := range parentMap[cur] {
						if !all[child] {
							all[child] = true
							queue = append(queue, child)
						}
					}
				}
				res := []string{}
				for id := range all {
					res = append(res, id)
				}
				return res
			}
			start := []string{}
			for id := range hideIds {
				start = append(start, id)
			}
			toHide := getAllDescendants(start)
			for _, id := range toHide {
				delete(entityMap, id)
			}
		}

		// rebuild children arrays using entity objects
		for _, e := range entityMap {
			e["children"] = []interface{}{}
		}
		for parentID, children := range parentMap {
			if entityMap[parentID] != nil {
				for _, childID := range children {
					if entityMap[childID] != nil {
						entityMap[parentID]["children"] = append(entityMap[parentID]["children"].([]interface{}), entityMap[childID])
					}
				}
			}
		}

		// collect top-level nodes (those not appearing as any child)
		childSet := map[string]bool{}
		for _, children := range parentMap {
			for _, childID := range children {
				childSet[childID] = true
			}
		}
		topLevel := []interface{}{}
		for _, e := range entityMap {
			if !childSet[e["id"].(string)] {
				topLevel = append(topLevel, e)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(topLevel)
	}
}

// FindParentCostProfitCenterAtLevel returns ancestors at a given level
func FindParentCostProfitCenterAtLevel(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Level  int    `json:"level"`
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

		parentLevel := req.Level - 1
		q := `
			SELECT m.centre_name, m.centre_id
			FROM mastercostprofitcenter m
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactioncostprofitcenter a
				WHERE a.centre_id = m.centre_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE m.centre_level = $1
			  AND (m.is_deleted = false OR m.is_deleted IS NULL)
			  AND LOWER(m.status) = 'active'
			  AND a.processing_status = 'APPROVED'
		`
		rows, err := pgxPool.Query(context.Background(), q, parentLevel)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		results := []map[string]interface{}{}
		for rows.Next() {
			var name, id string
			if err := rows.Scan(&name, &id); err == nil {
				results = append(results, map[string]interface{}{"centre_name": name, "centre_id": id})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "results": results})
	}
}

// DeleteCostProfitCenter inserts a DELETE audit action (no hard delete here)
func DeleteCostProfitCenter(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			CentreIDs []string `json:"centre_ids"`
			Reason    string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		if len(req.CentreIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "centre_ids required")
			return
		}

		// Bulk insert audit actions for all provided centre ids. Use unnest on the text[] parameter.
		q := `INSERT INTO auditactioncostprofitcenter (centre_id, actiontype, processing_status, reason, requested_by, requested_at)
			  SELECT cid::uuid, 'DELETE', 'PENDING_DELETE_APPROVAL', $1, $2, now() FROM unnest($3::text[]) AS cid`
		if _, err := pgxPool.Exec(r.Context(), q, req.Reason, requestedBy, req.CentreIDs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
	}
}

// BulkRejectCostProfitCenterActions rejects latest audit actions for centre_ids
func BulkRejectCostProfitCenterActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			CentreIDs []string `json:"centre_ids"`
			Comment   string   `json:"comment"`
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
		// Fetch relationships and compute descendants, then update audit rows by centre_id
		ctx := context.Background()

		relRows, err := pgxPool.Query(ctx, `SELECT parent_centre_id, child_centre_id FROM costprofitcenterrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentID, childID string
			if err := relRows.Scan(&parentID, &childID); err == nil {
				parentMap[parentID] = append(parentMap[parentID], childID)
			}
		}

		// traverse descendants
		getAllDescendants := func(ids []string) []string {
			all := map[string]bool{}
			queue := append([]string{}, ids...)
			for _, id := range ids {
				all[id] = true
			}
			for len(queue) > 0 {
				current := queue[0]
				queue = queue[1:]
				for _, child := range parentMap[current] {
					if !all[child] {
						all[child] = true
						queue = append(queue, child)
					}
				}
			}
			res := []string{}
			for id := range all {
				res = append(res, id)
			}
			return res
		}

		allToReject := getAllDescendants(req.CentreIDs)
		if len(allToReject) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No centres found to reject")
			return
		}

		// Update audit rows to REJECTED and return affected rows
		query := `UPDATE auditactioncostprofitcenter SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE centre_id = ANY($3) RETURNING action_id, centre_id`
		rows2, err := pgxPool.Query(ctx, query, checkerBy, req.Comment, allToReject)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows2.Close()

		var updated []map[string]interface{}
		for rows2.Next() {
			var actionID, centreID string
			if err := rows2.Scan(&actionID, &centreID); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": actionID, "centre_id": centreID})
			}
		}

		w.Header().Set("Content-Type", "application/json")
		success := len(updated) > 0
		resp := map[string]interface{}{"success": success, "updated": updated}
		if !success {
			resp["message"] = "No rows updated"
		}
		json.NewEncoder(w).Encode(resp)
	}
}

// BulkApproveCostProfitCenterActions approves audit actions and deletes master rows for DELETE actions
func BulkApproveCostProfitCenterActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			CentreIDs []string `json:"centre_ids"`
			Comment   string   `json:"comment"`
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

		// Fetch relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_centre_id, child_centre_id FROM costprofitcenterrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentID, childID string
			if err := relRows.Scan(&parentID, &childID); err == nil {
				parentMap[parentID] = append(parentMap[parentID], childID)
			}
		}

		// Traverse descendants
		getAllDescendants := func(ids []string) []string {
			all := map[string]bool{}
			queue := append([]string{}, ids...)
			for _, id := range ids {
				all[id] = true
			}
			for len(queue) > 0 {
				current := queue[0]
				queue = queue[1:]
				for _, child := range parentMap[current] {
					if !all[child] {
						all[child] = true
						queue = append(queue, child)
					}
				}
			}
			res := []string{}
			for id := range all {
				res = append(res, id)
			}
			return res
		}

		allToApprove := getAllDescendants(req.CentreIDs)
		if len(allToApprove) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No centres found to approve")
			return
		}

		// Update audit rows to APPROVED and return affected rows including action type
		query := `UPDATE auditactioncostprofitcenter SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE centre_id = ANY($3) RETURNING action_id, centre_id, actiontype`
		rows, err := pgxPool.Query(ctx, query, checkerBy, req.Comment, allToApprove)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var updated []map[string]interface{}
		var deleteIDs []string
		for rows.Next() {
			var actionID, centreID, actionType string
			if err := rows.Scan(&actionID, &centreID, &actionType); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": actionID, "centre_id": centreID, "action_type": actionType})
				if strings.ToUpper(strings.TrimSpace(actionType)) == "DELETE" {
					deleteIDs = append(deleteIDs, centreID)
				}
			}
		}

		// Set is_deleted=true for approved DELETE actions
		if len(deleteIDs) > 0 {
			updQ := `UPDATE mastercostprofitcenter SET is_deleted=true WHERE centre_id = ANY($1)`
			if _, err := pgxPool.Exec(ctx, updQ, deleteIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to set is_deleted: "+err.Error())
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		success := len(updated) > 0
		resp := map[string]interface{}{"success": success, "updated": updated}
		if !success {
			resp["message"] = "No rows updated"
		}
		json.NewEncoder(w).Encode(resp)
	}
}
