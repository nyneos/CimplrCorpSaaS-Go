package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

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
	// New optional fields
	DefaultCurrency    string `json:"default_currency,omitempty"`
	Owner              string `json:"owner,omitempty"`
	OwnerEmail         string `json:"owner_email,omitempty"`
	EffectiveFrom      string `json:"effective_from,omitempty"` // Date in "YYYY-MM-DD" format
	EffectiveTo        string `json:"effective_to,omitempty"`   // Date in "YYYY-MM-DD" format
	Tags               string `json:"tags,omitempty"`
	ExternalCode       string `json:"external_code,omitempty"`
	Segment            string `json:"segment,omitempty"`
	SAPKOKRS           string `json:"sap_kokrs,omitempty"`
	SAPBUKRS           string `json:"sap_bukrs,omitempty"`
	SAPKOSTL           string `json:"sap_kostl,omitempty"`
	SAPPRCTR           string `json:"sap_prctr,omitempty"`
	OracleLedger       string `json:"oracle_ledger,omitempty"`
	OracleDept         string `json:"oracle_dept,omitempty"`
	OracleProfitCenter string `json:"oracle_profit_center,omitempty"`
	TallyLedgerName    string `json:"tally_ledger_name,omitempty"`
	TallyLedgerGroup   string `json:"tally_ledger_group,omitempty"`
	SageDeptCode       string `json:"sage_department_code,omitempty"`
	SageCostCentreCode string `json:"sage_cost_centre_code,omitempty"`
	// optional: if caller wants to override computed values
	CentreLevel      int  `json:"centre_level,omitempty"`
	IsTopLevelCentre bool `json:"is_top_level_centre,omitempty"`
}

func ifaceToDateString(val interface{}) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case time.Time:
		return v.Format("2006-01-02")
	case string:
		return v
	default:
		return fmt.Sprint(val)
	}
}
func NormalizeDate(dateStr string) string {
	dateStr = strings.TrimSpace(dateStr)
	if dateStr == "" {
		return ""
	}

	layouts := []string{
		"2006-01-02",
		"02-01-2006",
		"2006/01/02",
		"02/01/2006",
		"2006.01.02",
		"02.01.2006",
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	layouts = append(layouts, []string{
		"02-Jan-2006",
		"02-Jan-06",
		"2-Jan-2006",
		"2-Jan-06",
		"02-Jan-2006 15:04:05",
	}...)

	for _, l := range layouts {
		if t, err := time.Parse(l, dateStr); err == nil {
			return t.Format("2006-01-02")
		}
	}

	return ""
}
func CreateAndSyncCostProfitCenters(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string                    `json:"user_id"`
			Rows   []CostProfitCenterRequest `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON")
			return
		}

		// Find createdBy from active sessions
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

			// Compute parent and level
			centreLevel := rrow.CentreLevel
			isTop := rrow.IsTopLevelCentre
			if strings.TrimSpace(rrow.ParentCode) != "" {
				var plevel int
				if err := pgxPool.QueryRow(ctx, `SELECT centre_level FROM mastercostprofitcenter WHERE centre_code=$1`, rrow.ParentCode).Scan(&plevel); err == nil {
					centreLevel = plevel + 1
					isTop = false
				} else {
					created = append(created, map[string]interface{}{"success": false, "error": "parent_centre_code not found", "centre_code": rrow.CentreCode})
					continue
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
						if rerr := tx.Rollback(ctx); rerr != nil {
							log.Println("rollback failed:", rerr)
						}
					}
				}()

				// Let DB generate centre_id (default on mastercostprofitcenter). We'll capture it with RETURNING.
				var centreID string

				// Parse dates
				var effectiveFrom, effectiveTo interface{}
				if rrow.EffectiveFrom != "" {
					if norm := NormalizeDate(rrow.EffectiveFrom); norm != "" {
						if tval, err := time.Parse("2006-01-02", norm); err == nil {
							effectiveFrom = tval
						}
					}
				}
				if rrow.EffectiveTo != "" {
					if norm := NormalizeDate(rrow.EffectiveTo); norm != "" {
						if tval, err := time.Parse("2006-01-02", norm); err == nil {
							effectiveTo = tval
						}
					}
				}

				// Insert CPC (without centre_id so DB default applies) and capture generated centre_id
				ins := `INSERT INTO mastercostprofitcenter (
					centre_code, centre_name, centre_type, parent_centre_code, 
					entity_name, status, source, erp_type, centre_level, is_top_level_centre,
					default_currency, owner, owner_email, effective_from, effective_to, 
					tags, external_code, segment, sap_kokrs, sap_bukrs, sap_kostl, sap_prctr,
					oracle_ledger, oracle_dept, oracle_profit_center, tally_ledger_name, 
					tally_ledger_group, sage_department_code, sage_cost_centre_code
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)
				RETURNING centre_id`

				if err := tx.QueryRow(ctx, ins,
					rrow.CentreCode,
					rrow.CentreName,
					rrow.CentreType,
					rrow.ParentCode,
					rrow.EntityCode,
					rrow.Status,
					rrow.Source,
					rrow.ErpRef,
					centreLevel,
					isTop,
					rrow.DefaultCurrency,
					rrow.Owner,
					rrow.OwnerEmail,
					effectiveFrom,
					effectiveTo,
					rrow.Tags,
					rrow.ExternalCode,
					rrow.Segment,
					rrow.SAPKOKRS,
					rrow.SAPBUKRS,
					rrow.SAPKOSTL,
					rrow.SAPPRCTR,
					rrow.OracleLedger,
					rrow.OracleDept,
					rrow.OracleProfitCenter,
					rrow.TallyLedgerName,
					rrow.TallyLedgerGroup,
					rrow.SageDeptCode,
					rrow.SageCostCentreCode,
				).Scan(&centreID); err != nil {
					created = append(created, map[string]interface{}{"success": false, "error": err.Error(), "centre_code": rrow.CentreCode})
					return
				}
				if err != nil {
					created = append(created, map[string]interface{}{"success": false, "error": err.Error(), "centre_code": rrow.CentreCode})
					return
				}

				// Insert relationship if parent exists. Relationships table now stores centre codes (parent_centre_code, child_centre_code)
				if strings.TrimSpace(rrow.ParentCode) != "" {
					relQ := `INSERT INTO costprofitcenterrelationships (parent_centre_code, child_centre_code)
							 VALUES ($1, $2)
							 ON CONFLICT (parent_centre_code, child_centre_code) DO NOTHING`
					if _, err := tx.Exec(ctx, relQ, rrow.ParentCode, rrow.CentreCode); err != nil {
						created = append(created, map[string]interface{}{"success": false, "error": "relationship insert failed: " + err.Error(), "centre_code": rrow.CentreCode})
						return
					}
				}

				// Insert audit
				auditQ := `INSERT INTO auditactioncostprofitcenter (centre_id, actiontype, processing_status, reason, requested_by, requested_at)
                           VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
				if _, err := tx.Exec(ctx, auditQ, centreID, nil, createdBy); err != nil {
					created = append(created, map[string]interface{}{"success": false, "error": "audit insert failed: " + err.Error(), "centre_id": centreID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					created = append(created, map[string]interface{}{"success": false, "error": "commit failed: " + err.Error(), "centre_id": centreID})
					return
				}
				committed = true

				created = append(created, map[string]interface{}{
					"success":             true,
					"centre_id":           centreID,
					"centre_code":         rrow.CentreCode,
					"centre_level":        centreLevel,
					"is_top_level_centre": isTop,
				})
			}()
		}

		overall := api.IsBulkSuccess(created)
		api.RespondWithPayload(w, overall, "", created)
	}
}
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
				results = append(results, map[string]interface{}{"success": false, "error": "missing centre_id", "centre_id": row.CentreID})
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "begin failed: " + err.Error(), "centre_id": row.CentreID})
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
				var existingCode, existingName, existingType, existingParentCode, existingEntity, existingStatus, existingSource, existingErp interface{}
				var existingLevel interface{}
				var existingIsTop interface{}
				var existingCurrency, existingOwner, existingOwnerEmail interface{}
				var existingEffFrom, existingEffTo, existingTags, existingExtCode, existingSegment interface{}
				var existingSAPKOKRS, existingSAPBUKRS, existingSAPKOSTL, existingSAPPRCTR interface{}
				var existingOracleLedger, existingOracleDept, existingOraclePC interface{}
				var existingTallyName, existingTallyGroup interface{}
				var existingSageDept, existingSageCost interface{}

				qSel := `SELECT 
					centre_code, centre_name, centre_type, parent_centre_code, entity_name, 
					status, source, erp_type, centre_level, is_top_level_centre,
					default_currency, owner, owner_email, effective_from, effective_to,
					tags, external_code, segment, sap_kokrs, sap_bukrs, sap_kostl, sap_prctr,
					oracle_ledger, oracle_dept, oracle_profit_center, tally_ledger_name,
					tally_ledger_group, sage_department_code, sage_cost_centre_code
				FROM mastercostprofitcenter WHERE centre_id = $1 FOR UPDATE`

				if err := tx.QueryRow(ctx, qSel, row.CentreID).Scan(
					&existingCode, &existingName, &existingType, &existingParentCode, &existingEntity,
					&existingStatus, &existingSource, &existingErp, &existingLevel, &existingIsTop,
					&existingCurrency, &existingOwner, &existingOwnerEmail, &existingEffFrom, &existingEffTo,
					&existingTags, &existingExtCode, &existingSegment, &existingSAPKOKRS, &existingSAPBUKRS,
					&existingSAPKOSTL, &existingSAPPRCTR, &existingOracleLedger, &existingOracleDept,
					&existingOraclePC, &existingTallyName, &existingTallyGroup, &existingSageDept, &existingSageCost,
				); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "fetch failed: " + err.Error(), "centre_id": row.CentreID})
					return
				}

				var sets []string
				var args []interface{}
				pos := 1
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
					case "erp_ref", "erp_type":
						sets = append(sets, fmt.Sprintf("erp_type=$%d, old_erp_type=$%d", pos, pos+1))
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
							computedLevel = 0
							computedIsTop = true
						} else {
							var plevel int
							if err := pgxPool.QueryRow(ctx, `SELECT centre_level FROM mastercostprofitcenter WHERE centre_code=$1`, pcode).Scan(&plevel); err != nil {
								results = append(results, map[string]interface{}{"success": false, "error": "parent centre not found: " + pcode, "centre_id": row.CentreID})
								return
							}
							computedLevel = plevel + 1
							computedIsTop = false
						}
						// set parent and old_parent and update level fields
						sets = append(sets, fmt.Sprintf("parent_centre_code=$%d, old_parent_centre_code=$%d, centre_level=$%d, old_centre_level=$%d, is_top_level_centre=$%d", pos, pos+1, pos+2, pos+3, pos+4))
						args = append(args, pcode, ifaceToString(existingParentCode), computedLevel, existingLevel, computedIsTop)
						pos += 5

					// New fields - add old_* values for tracking changes
					case "default_currency":
						sets = append(sets, fmt.Sprintf("default_currency=$%d, old_default_currency=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCurrency))
						pos += 2
					case "owner":
						sets = append(sets, fmt.Sprintf("owner=$%d, old_owner=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOwner))
						pos += 2
					case "owner_email":
						sets = append(sets, fmt.Sprintf("owner_email=$%d, old_owner_email=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOwnerEmail))
						pos += 2
					case "effective_from":
						// Handle date parsing
						dateStr := strings.TrimSpace(fmt.Sprint(v))
						var dateVal interface{}
						if dateStr != "" {
							if date, err := time.Parse("2006-01-02", dateStr); err == nil {
								dateVal = date
							}
						}
						sets = append(sets, fmt.Sprintf("effective_from=$%d, old_effective_from=$%d", pos, pos+1))
						args = append(args, dateVal, existingEffFrom)
						pos += 2
					case "effective_to":
						// Handle date parsing
						dateStr := strings.TrimSpace(fmt.Sprint(v))
						var dateVal interface{}
						if dateStr != "" {
							if date, err := time.Parse("2006-01-02", dateStr); err == nil {
								dateVal = date
							}
						}
						sets = append(sets, fmt.Sprintf("effective_to=$%d, old_effective_to=$%d", pos, pos+1))
						args = append(args, dateVal, existingEffTo)
						pos += 2
					case "tags":
						sets = append(sets, fmt.Sprintf("tags=$%d, old_tags=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTags))
						pos += 2
					case "external_code":
						sets = append(sets, fmt.Sprintf("external_code=$%d, old_external_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingExtCode))
						pos += 2
					case "segment":
						sets = append(sets, fmt.Sprintf("segment=$%d, old_segment=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSegment))
						pos += 2
					case "sap_kokrs":
						sets = append(sets, fmt.Sprintf("sap_kokrs=$%d, old_sap_kokrs=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSAPKOKRS))
						pos += 2
					case "sap_bukrs":
						sets = append(sets, fmt.Sprintf("sap_bukrs=$%d, old_sap_bukrs=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSAPBUKRS))
						pos += 2
					case "sap_kostl":
						sets = append(sets, fmt.Sprintf("sap_kostl=$%d, old_sap_kostl=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSAPKOSTL))
						pos += 2
					case "sap_prctr":
						sets = append(sets, fmt.Sprintf("sap_prctr=$%d, old_sap_prctr=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSAPPRCTR))
						pos += 2
					case "oracle_ledger":
						sets = append(sets, fmt.Sprintf("oracle_ledger=$%d, old_oracle_ledger=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleLedger))
						pos += 2
					case "oracle_dept":
						sets = append(sets, fmt.Sprintf("oracle_dept=$%d, old_oracle_dept=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleDept))
						pos += 2
					case "oracle_profit_center":
						sets = append(sets, fmt.Sprintf("oracle_profit_center=$%d, old_oracle_profit_center=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOraclePC))
						pos += 2
					case "tally_ledger_name":
						sets = append(sets, fmt.Sprintf("tally_ledger_name=$%d, old_tally_ledger_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTallyName))
						pos += 2
					case "tally_ledger_group":
						sets = append(sets, fmt.Sprintf("tally_ledger_group=$%d, old_tally_ledger_group=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTallyGroup))
						pos += 2
					case "sage_department_code":
						sets = append(sets, fmt.Sprintf("sage_department_code=$%d, old_sage_department_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSageDept))
						pos += 2
					case "sage_cost_centre_code":
						sets = append(sets, fmt.Sprintf("sage_cost_centre_code=$%d, old_sage_cost_centre_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSageCost))
						pos += 2
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

				// sync relationships when parent changed - now comparing centre codes
				if computedLevel != nil || computedIsTop != nil {
					// Parent was changed, sync the relationships
					// Get current centre_code for this centre
					var currentCentreCode string
					if err := tx.QueryRow(ctx, `SELECT centre_code FROM mastercostprofitcenter WHERE centre_id=$1`, updatedID).Scan(&currentCentreCode); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "failed to get centre code: " + err.Error(), "centre_id": updatedID})
						return
					}

					// remove existing relationships for this child
					delQ := `DELETE FROM costprofitcenterrelationships WHERE child_centre_code=$1`
					if _, err := tx.Exec(ctx, delQ, currentCentreCode); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "failed to remove old relationships: " + err.Error(), "centre_id": updatedID})
						return
					}

					// insert new relationship if parent exists
					newParentCode := ""
					for k, v := range row.Fields {
						if k == "parent_centre_code" {
							newParentCode = strings.TrimSpace(fmt.Sprint(v))
							break
						}
					}
					if newParentCode != "" {
						relQ := `INSERT INTO costprofitcenterrelationships (parent_centre_code, child_centre_code) VALUES ($1, $2) ON CONFLICT (parent_centre_code, child_centre_code) DO NOTHING`
						if _, err := tx.Exec(ctx, relQ, newParentCode, currentCentreCode); err != nil {
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
		overall := api.IsBulkSuccess(results)
		api.RespondWithPayload(w, overall, "", results)
	}
}

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
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
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

		// DB will generate centre_id by default; no client-side sequence required

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
			// regex to insert underscore between a lower/number and upper case letter
			camelRe := regexp.MustCompile(`([a-z0-9])([A-Z])`)
			// regex to collapse multiple non-alphanumeric into single underscore
			nonAlnumRe := regexp.MustCompile(`[^a-zA-Z0-9]+`)

			for i, h := range headerRow {
				hn := strings.TrimSpace(h)
				hn = strings.Trim(hn, ", ")
				hn = strings.Trim(hn, "\"'`")
				if hn == "" {
					headerNorm[i] = hn
					continue
				}
				// First, normalize camelCase/PascalCase -> insert underscore between lowercase/number and uppercase
				hn = camelRe.ReplaceAllString(hn, `${1}_${2}`)
				// Replace any remaining non-alphanumeric sequences with underscore
				hn = nonAlnumRe.ReplaceAllString(hn, "_")
				// Lowercase
				hn = strings.ToLower(hn)
				// Trim leading/trailing underscores
				hn = strings.Trim(hn, "_")
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
				api.RespondWithResult(w, false, "Failed to stage data: "+err.Error())
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

			// Insert into master table letting DB set centre_id; capture centre_id and centre_code for inserted rows
			tgtColsStr := strings.Join(tgtCols, ", ")
			var selectExprs []string
			for i, src := range srcCols {
				tgt := tgtCols[i]
				selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", src, tgt))
			}
			srcColsStr := strings.Join(selectExprs, ", ")

			// Build insert: don't include centre_id column (DB default will populate it). Use ON CONFLICT DO NOTHING.
			insertSQL := fmt.Sprintf(`INSERT INTO mastercostprofitcenter (%s) SELECT %s FROM input_costprofitcenter s WHERE s.upload_batch_id = $1 ON CONFLICT (centre_code) DO NOTHING RETURNING centre_id, centre_code`, tgtColsStr, srcColsStr)

			insertedRows, err := tx.Query(ctx, insertSQL, batchID)
			if err != nil {
				api.RespondWithResult(w, false, "Final insert error: "+err.Error())
				return
			}
			defer insertedRows.Close()

			codeToID := make(map[string]string)
			for insertedRows.Next() {
				var id, code string
				if err := insertedRows.Scan(&id, &code); err == nil {
					codeToID[code] = id
				}
			}

			// Also include any pre-existing centre_codes that were not inserted due to conflict
			// so relationships can be created for those rows as well.
			existingRows, err := tx.Query(ctx, `
				SELECT m.centre_id, m.centre_code
				FROM mastercostprofitcenter m
				WHERE m.centre_code IN (
					SELECT DISTINCT centre_code FROM input_costprofitcenter WHERE upload_batch_id = $1
				)
			`, batchID)
			if err == nil {
				defer existingRows.Close()
				for existingRows.Next() {
					var id, code string
					if err := existingRows.Scan(&id, &code); err == nil {
						if _, ok := codeToID[code]; !ok {
							codeToID[code] = id
						}
					}
				}
			}

			// create relationships from input rows using the actual source columns that map to centre_code and parent info
			// find source column names for centre_code and parent (if mapped)
			childSrcCol := "centre_code"
			parentSrcCol := "parent_centre_code"
			// try to locate in mapping: srcCols[i] maps to tgtCols[i]
			for i, tgt := range tgtCols {
				lt := strings.ToLower(strings.TrimSpace(tgt))
				if lt == "centre_code" {
					childSrcCol = srcCols[i]
				}
				// accept multiple possible parent target names
				if lt == "parent_centre_code" || lt == "parent_centre" || lt == "parent" {
					parentSrcCol = srcCols[i]
				}
			}

			// If parentSrcCol not present in headerNorm, attempt fallback to a common parent column name
			foundParent := false
			for _, hn := range headerNorm {
				if hn == parentSrcCol {
					foundParent = true
					break
				}
			}
			if !foundParent {
				// try common fallback names
				for _, cand := range []string{"parent_centre_code", "parent_centre", "parent"} {
					for _, hn := range headerNorm {
						if hn == cand {
							parentSrcCol = cand
							foundParent = true
							break
						}
					}
					if foundParent {
						break
					}
				}
			}

			relQuery := fmt.Sprintf("SELECT %s, %s FROM input_costprofitcenter WHERE upload_batch_id=$1", childSrcCol, parentSrcCol)
			relRows, err := tx.Query(ctx, relQuery, batchID)
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
					if cc == "" {
						continue
					}

					// Resolve child ID: prefer newly inserted map, fallback to existing master
					var childID string
					if cid, ok := codeToID[cc]; ok {
						childID = cid
					} else {
						if err := tx.QueryRow(ctx, `SELECT centre_id FROM mastercostprofitcenter WHERE centre_code=$1`, cc).Scan(&childID); err != nil {
							// cannot resolve child - skip
							continue
						}
					}

					if pc != "" {
						// Resolve parent ID: prefer current batch map, then existing master by code
						var parentID string
						if pid, ok := codeToID[pc]; ok {
							parentID = pid
						} else {
							if err := tx.QueryRow(ctx, `SELECT centre_id FROM mastercostprofitcenter WHERE centre_code=$1`, pc).Scan(&parentID); err != nil {
								// cannot resolve parent - skip relationship
								continue
							}
						}

						// Insert relationship (no-op if exists)
						relQ := `INSERT INTO costprofitcenterrelationships (parent_centre_code, child_centre_code) VALUES ($1,$2) ON CONFLICT DO NOTHING`
						if _, err := tx.Exec(ctx, relQ, pc, cc); err != nil {
							tx.Rollback(ctx)
							api.RespondWithResult(w, false, "relationship insert failed: "+err.Error())
							return
						}

						// Update child master: set parent and compute level
						var parentLevel int
						if err := tx.QueryRow(ctx, `SELECT centre_level FROM mastercostprofitcenter WHERE centre_code=$1`, pc).Scan(&parentLevel); err == nil {
							_, _ = tx.Exec(ctx, `UPDATE mastercostprofitcenter SET parent_centre_code=$1, centre_level=$2, is_top_level_centre=false WHERE centre_id=$3`, pc, parentLevel+1, childID)
						} else {
							// parent exists but level unknown - at least set parent code
							_, _ = tx.Exec(ctx, `UPDATE mastercostprofitcenter SET parent_centre_code=$1 WHERE centre_id=$2`, pc, childID)
						}
					} else {
						// No parent specified: ensure this child is marked top-level with level 0
						_, _ = tx.Exec(ctx, `UPDATE mastercostprofitcenter SET parent_centre_code=NULL, centre_level=0, is_top_level_centre=true WHERE centre_id=$1`, childID)
					}
				}
			}

			// Create audit entries for the newly inserted rows
			var centreIDs []string
			for _, id := range codeToID {
				centreIDs = append(centreIDs, id)
			}
			if len(centreIDs) > 0 {
				auditSQL := `INSERT INTO auditactioncostprofitcenter (centre_id, actiontype, processing_status, reason, requested_by, requested_at) 
							SELECT centre_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now() 
							FROM mastercostprofitcenter WHERE centre_id = ANY($2)`
				if _, err := tx.Exec(ctx, auditSQL, userEmail, centreIDs); err != nil {
					api.RespondWithResult(w, false, "Failed to insert audit actions: "+err.Error())
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
            SELECT 
                m.centre_id, m.centre_code, m.centre_name, m.centre_type, m.parent_centre_code, 
                m.entity_name, m.status, m.source, m.erp_type,
                m.old_centre_code, m.old_centre_name, m.old_centre_type, m.old_parent_centre_code, 
                m.old_entity_name, m.old_status, m.old_source, m.old_erp_type,
                m.centre_level, m.old_centre_level, m.is_top_level_centre, m.is_deleted,
                m.default_currency, m.old_default_currency,
                m.owner, m.old_owner,
                m.owner_email, m.old_owner_email,
                m.effective_from, m.old_effective_from,
                m.effective_to, m.old_effective_to,
                m.tags, m.old_tags,
                m.external_code, m.old_external_code,
                m.segment, m.old_segment,
                m.sap_kokrs, m.old_sap_kokrs,
                m.sap_bukrs, m.old_sap_bukrs,
                m.sap_kostl, m.old_sap_kostl,
                m.sap_prctr, m.old_sap_prctr,
                m.oracle_ledger, m.old_oracle_ledger,
                m.oracle_dept, m.old_oracle_dept,
                m.oracle_profit_center, m.old_oracle_profit_center,
                m.tally_ledger_name, m.old_tally_ledger_name,
                m.tally_ledger_group, m.old_tally_ledger_group,
                m.sage_department_code, m.old_sage_department_code,
                m.sage_cost_centre_code, m.old_sage_cost_centre_code,
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
			api.RespondWithResult(w, false, err.Error())
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
				centreCodeI, centreNameI, centreTypeI, parentCentreIDI, entityNameI, statusI, sourceI, erpTypeI,
				oldCodeI, oldNameI, oldTypeI, oldParentI, oldEntityI, oldStatusI, oldSourceI, oldErpTypeI,
				centreLevelI, oldCentreLevelI, procStatusI, requestedByI, requestedAtI, actionTypeI,
				checkerByI, checkerAtI, checkerCommentI, reasonI interface{}
				isTopLevel, isDeleted bool

				// New fields
				defaultCurrencyI, oldDefaultCurrencyI,
				ownerI, oldOwnerI,
				ownerEmailI, oldOwnerEmailI,
				effectiveFromI, oldEffectiveFromI,
				effectiveToI, oldEffectiveToI,
				tagsI, oldTagsI,
				externalCodeI, oldExternalCodeI,
				segmentI, oldSegmentI,
				sapKOKRSI, oldSapKOKRSI,
				sapBUKRSI, oldSapBUKRSI,
				sapKOSTLI, oldSapKOSTLI,
				sapPRCTRI, oldSapPRCTRI,
				oracleLedgerI, oldOracleLedgerI,
				oracleDeptI, oldOracleDeptI,
				oracleProfitCenterI, oldOracleProfitCenterI,
				tallyLedgerNameI, oldTallyLedgerNameI,
				tallyLedgerGroupI, oldTallyLedgerGroupI,
				sageDeptCodeI, oldSageDeptCodeI,
				sageCostCentreCodeI, oldSageCostCentreCodeI interface{}
			)

			if err := rows.Scan(
				&centreID, &centreCodeI, &centreNameI, &centreTypeI, &parentCentreIDI,
				&entityNameI, &statusI, &sourceI, &erpTypeI,
				&oldCodeI, &oldNameI, &oldTypeI, &oldParentI, &oldEntityI, &oldStatusI, &oldSourceI, &oldErpTypeI,
				&centreLevelI, &oldCentreLevelI, &isTopLevel, &isDeleted,
				&defaultCurrencyI, &oldDefaultCurrencyI,
				&ownerI, &oldOwnerI,
				&ownerEmailI, &oldOwnerEmailI,
				&effectiveFromI, &oldEffectiveFromI,
				&effectiveToI, &oldEffectiveToI,
				&tagsI, &oldTagsI,
				&externalCodeI, &oldExternalCodeI,
				&segmentI, &oldSegmentI,
				&sapKOKRSI, &oldSapKOKRSI,
				&sapBUKRSI, &oldSapBUKRSI,
				&sapKOSTLI, &oldSapKOSTLI,
				&sapPRCTRI, &oldSapPRCTRI,
				&oracleLedgerI, &oldOracleLedgerI,
				&oracleDeptI, &oldOracleDeptI,
				&oracleProfitCenterI, &oldOracleProfitCenterI,
				&tallyLedgerNameI, &oldTallyLedgerNameI,
				&tallyLedgerGroupI, &oldTallyLedgerGroupI,
				&sageDeptCodeI, &oldSageDeptCodeI,
				&sageCostCentreCodeI, &oldSageCostCentreCodeI,
				&procStatusI, &requestedByI, &requestedAtI, &actionTypeI, &actionIDI,
				&checkerByI, &checkerAtI, &checkerCommentI, &reasonI); err != nil {
				continue
			}

			entityMap[centreID] = map[string]interface{}{
				"id":   centreID,
				"name": ifaceToString(centreNameI),
				"data": map[string]interface{}{
					// Basic fields
					"centre_id":          centreID,
					"centre_code":        ifaceToString(centreCodeI),
					"centre_name":        ifaceToString(centreNameI),
					"centre_type":        ifaceToString(centreTypeI),
					"parent_centre_code": ifaceToString(parentCentreIDI),
					"entity_name":        ifaceToString(entityNameI),
					"status":             ifaceToString(statusI),
					"source":             ifaceToString(sourceI),
					"erp_type":           ifaceToString(erpTypeI),

					// Old values
					"old_centre_code":        ifaceToString(oldCodeI),
					"old_centre_name":        ifaceToString(oldNameI),
					"old_centre_type":        ifaceToString(oldTypeI),
					"old_parent_centre_code": ifaceToString(oldParentI),
					"old_entity_name":        ifaceToString(oldEntityI),
					"old_status":             ifaceToString(oldStatusI),
					"old_source":             ifaceToString(oldSourceI),
					"old_erp_type":           ifaceToString(oldErpTypeI),

					// Level and flags
					"centre_level":        ifaceToInt(centreLevelI),
					"old_centre_level":    ifaceToInt(oldCentreLevelI),
					"is_top_level_centre": isTopLevel,
					"is_deleted":          isDeleted,

					// New fields
					"default_currency":     ifaceToString(defaultCurrencyI),
					"old_default_currency": ifaceToString(oldDefaultCurrencyI),
					"owner":                ifaceToString(ownerI),
					"old_owner":            ifaceToString(oldOwnerI),
					"owner_email":          ifaceToString(ownerEmailI),
					"old_owner_email":      ifaceToString(oldOwnerEmailI),
					"effective_from":       ifaceToDateString(effectiveFromI),
					"old_effective_from":   ifaceToDateString(oldEffectiveFromI),
					"effective_to":         ifaceToDateString(effectiveToI),
					"old_effective_to":     ifaceToDateString(oldEffectiveToI),
					"tags":                 ifaceToString(tagsI),
					"old_tags":             ifaceToString(oldTagsI),
					"external_code":        ifaceToString(externalCodeI),
					"old_external_code":    ifaceToString(oldExternalCodeI),
					"segment":              ifaceToString(segmentI),
					"old_segment":          ifaceToString(oldSegmentI),

					// SAP fields
					"sap_kokrs":     ifaceToString(sapKOKRSI),
					"old_sap_kokrs": ifaceToString(oldSapKOKRSI),
					"sap_bukrs":     ifaceToString(sapBUKRSI),
					"old_sap_bukrs": ifaceToString(oldSapBUKRSI),
					"sap_kostl":     ifaceToString(sapKOSTLI),
					"old_sap_kostl": ifaceToString(oldSapKOSTLI),
					"sap_prctr":     ifaceToString(sapPRCTRI),
					"old_sap_prctr": ifaceToString(oldSapPRCTRI),

					// Oracle fields
					"oracle_ledger":            ifaceToString(oracleLedgerI),
					"old_oracle_ledger":        ifaceToString(oldOracleLedgerI),
					"oracle_dept":              ifaceToString(oracleDeptI),
					"old_oracle_dept":          ifaceToString(oldOracleDeptI),
					"oracle_profit_center":     ifaceToString(oracleProfitCenterI),
					"old_oracle_profit_center": ifaceToString(oldOracleProfitCenterI),

					// Tally fields
					"tally_ledger_name":      ifaceToString(tallyLedgerNameI),
					"old_tally_ledger_name":  ifaceToString(oldTallyLedgerNameI),
					"tally_ledger_group":     ifaceToString(tallyLedgerGroupI),
					"old_tally_ledger_group": ifaceToString(oldTallyLedgerGroupI),

					// Sage fields
					"sage_department_code":      ifaceToString(sageDeptCodeI),
					"old_sage_department_code":  ifaceToString(oldSageDeptCodeI),
					"sage_cost_centre_code":     ifaceToString(sageCostCentreCodeI),
					"old_sage_cost_centre_code": ifaceToString(oldSageCostCentreCodeI),

					// Audit fields
					"processing_status": ifaceToString(procStatusI),
					"action_type":       ifaceToString(actionTypeI),
					"action_id":         actionIDI,
					"checker_by":        ifaceToString(checkerByI),
					"checker_at":        ifaceToTimeString(checkerAtI),
					"checker_comment":   ifaceToString(checkerCommentI),
					"reason":            ifaceToString(reasonI),
					"requested_by":      ifaceToString(requestedByI),
					"requested_at":      ifaceToTimeString(requestedAtI),
				},
				"children": []interface{}{},
			}

			centreIDs = append(centreIDs, centreID)
			if isDeleted && strings.ToUpper(ifaceToString(procStatusI)) == "APPROVED" {
				hideIds[centreID] = true
			}
		}
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
		// relationships table stores centre codes now; fetch codes and resolve to IDs
		relRows, err := pgxPool.Query(ctx, `SELECT parent_centre_code, child_centre_code FROM costprofitcenterrelationships`)
		if err != nil {
			api.RespondWithResult(w, false, err.Error())
			return
		}
		defer relRows.Close()
		// parentMap maps parentCentreID -> []childCentreID
		parentMap := map[string][]string{}
		// We'll need to resolve codes to IDs; cache lookups
		codeToID := map[string]string{}
		for relRows.Next() {
			var pcode, ccode string
			if err := relRows.Scan(&pcode, &ccode); err == nil {
				// resolve parent code to id
				if _, ok := codeToID[pcode]; !ok {
					var pid string
					if err := pgxPool.QueryRow(ctx, `SELECT centre_id FROM mastercostprofitcenter WHERE centre_code=$1`, pcode).Scan(&pid); err == nil {
						codeToID[pcode] = pid
					} else {
						// skip if parent code not found
						continue
					}
				}
				if _, ok := codeToID[ccode]; !ok {
					var cid string
					if err := pgxPool.QueryRow(ctx, `SELECT centre_id FROM mastercostprofitcenter WHERE centre_code=$1`, ccode).Scan(&cid); err == nil {
						codeToID[ccode] = cid
					} else {
						// skip if child not found
						continue
					}
				}
				parentMap[codeToID[pcode]] = append(parentMap[codeToID[pcode]], codeToID[ccode])
			}
		}
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
		api.RespondWithPayload(w, true, "", topLevel)
	}
}

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
			SELECT m.centre_name, m.centre_id, m.centre_code
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
			var name, id, code string
			if err := rows.Scan(&name, &id, &code); err == nil {
				results = append(results, map[string]interface{}{"centre_name": name, "centre_id": id, "centre_code": code})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "results": results})
	}
}

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

		// Fetch relationships and compute descendants so we add audit actions for all descendants as well
		ctx := r.Context()
		relRows, err := pgxPool.Query(ctx, `SELECT parent_centre_code, child_centre_code FROM costprofitcenterrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		codeToIDMap := map[string]string{}

		// First get all centre_code to centre_id mappings
		codeRows, err := pgxPool.Query(ctx, `SELECT centre_id, centre_code FROM mastercostprofitcenter`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer codeRows.Close()
		for codeRows.Next() {
			var id, code string
			if err := codeRows.Scan(&id, &code); err == nil {
				codeToIDMap[code] = id
			}
		}

		// Build parent map using centre IDs
		for relRows.Next() {
			var parentCode, childCode string
			if err := relRows.Scan(&parentCode, &childCode); err == nil {
				if parentID, ok := codeToIDMap[parentCode]; ok {
					if childID, ok := codeToIDMap[childCode]; ok {
						parentMap[parentID] = append(parentMap[parentID], childID)
					}
				}
			}
		}

		// BFS to collect all descendants of provided centre ids
		allSet := map[string]bool{}
		queue := append([]string{}, req.CentreIDs...)
		for _, id := range req.CentreIDs {
			allSet[id] = true
		}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for _, child := range parentMap[cur] {
				if !allSet[child] {
					allSet[child] = true
					queue = append(queue, child)
				}
			}
		}

		if len(allSet) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No centres found to delete")
			return
		}

		allList := make([]string, 0, len(allSet))
		for id := range allSet {
			allList = append(allList, id)
		}

		// Bulk insert audit actions for all centre ids (roots + descendants)
		q := `INSERT INTO auditactioncostprofitcenter (centre_id, actiontype, processing_status, reason, requested_by, requested_at)
			  SELECT cid, 'DELETE', 'PENDING_DELETE_APPROVAL', $1, $2, now() FROM unnest($3::text[]) AS cid`
		if _, err := pgxPool.Exec(ctx, q, req.Reason, requestedBy, allList); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "queued_count": len(allList)})
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

		relRows, err := pgxPool.Query(ctx, `SELECT parent_centre_code, child_centre_code FROM costprofitcenterrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		codeToIDMap := map[string]string{}

		// Get centre_code to centre_id mappings
		codeRows, err := pgxPool.Query(ctx, `SELECT centre_id, centre_code FROM mastercostprofitcenter`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer codeRows.Close()
		for codeRows.Next() {
			var id, code string
			if err := codeRows.Scan(&id, &code); err == nil {
				codeToIDMap[code] = id
			}
		}

		// Build parent map using centre IDs
		for relRows.Next() {
			var parentCode, childCode string
			if err := relRows.Scan(&parentCode, &childCode); err == nil {
				if parentID, ok := codeToIDMap[parentCode]; ok {
					if childID, ok := codeToIDMap[childCode]; ok {
						parentMap[parentID] = append(parentMap[parentID], childID)
					}
				}
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
		relRows, err := pgxPool.Query(ctx, `SELECT parent_centre_code, child_centre_code FROM costprofitcenterrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		codeToIDMap := map[string]string{}

		// Get centre_code to centre_id mappings
		codeRows, err := pgxPool.Query(ctx, `SELECT centre_id, centre_code FROM mastercostprofitcenter`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer codeRows.Close()
		for codeRows.Next() {
			var id, code string
			if err := codeRows.Scan(&id, &code); err == nil {
				codeToIDMap[code] = id
			}
		}

		// Build parent map using centre IDs
		for relRows.Next() {
			var parentCode, childCode string
			if err := relRows.Scan(&parentCode, &childCode); err == nil {
				if parentID, ok := codeToIDMap[parentCode]; ok {
					if childID, ok := codeToIDMap[childCode]; ok {
						parentMap[parentID] = append(parentMap[parentID], childID)
					}
				}
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

// UploadCostProfitCenterSimple is a lightweight ingestion endpoint that copies
// CSV directly into mastercostprofitcenter and runs hierarchy sync + audit
// asynchronously to avoid blocking the HTTP request.
func UploadCostProfitCenterSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		userID := r.FormValue("user_id")
		if userID == "" {
			var req struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			userID = req.UserID
		}
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id required")
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
			api.RespondWithError(w, http.StatusUnauthorized, "User not found in active sessions")
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse form: "+err.Error())
			return
		}

		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No file uploaded")
			return
		}

		allowed := map[string]bool{
			"centre_code": true, "centre_name": true, "centre_type": true,
			"parent_centre_code": true, "entity_name": true, "status": true, "source": true,
			"default_currency": true, "erp_type": true, "owner": true, "owner_email": true,
			"effective_from": true, "effective_to": true, "tags": true,
			"segment": true, "sap_kokrs": true, "sap_bukrs": true,
			"sap_kostl": true, "sap_prctr": true, "oracle_ledger": true,
			"oracle_dept": true, "oracle_profit_center": true,
			"tally_ledger_name": true, "tally_ledger_group": true,
			"sage_department_code": true, "sage_cost_centre_code": true,
			"centre_level": true, "is_top_level_centre": true, "is_deleted": true,
		}

		batchIDs := []string{}
		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file")
				return
			}
			records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid CSV file")
				return
			}

			headers := make([]string, len(records[0]))
			for i, h := range records[0] {
				h = strings.ToLower(strings.TrimSpace(strings.ReplaceAll(h, " ", "_")))
				h = strings.Trim(h, "\"',")
				headers[i] = h
			}

			validCols := []string{}
			for _, h := range headers {
				if allowed[h] {
					validCols = append(validCols, h)
				}
			}
			if !(contains(validCols, "centre_code") && contains(validCols, "centre_name") && contains(validCols, "centre_type")) {
				api.RespondWithError(w, http.StatusBadRequest, "CSV must include centre_code, centre_name, centre_type")
				return
			}

			rows := records[1:]
			copyRows := make([][]interface{}, len(rows))
			// collect centre_codes from the CSV so async audit can locate the inserted rows
			centreCodes := make([]string, 0, len(rows))
			headerPos := map[string]int{}
			for i, h := range headers {
				headerPos[h] = i
			}

			for i, row := range rows {
				vals := make([]interface{}, len(validCols))
				for j, c := range validCols {
					if pos, ok := headerPos[c]; ok && pos < len(row) {
						cell := strings.TrimSpace(row[pos])
						if cell == "" {
							vals[j] = nil
						} else if c == "effective_from" || c == "effective_to" {
							if norm := NormalizeDate(cell); norm != "" {
								if t, err := time.Parse("2006-01-02", norm); err == nil {
									vals[j] = t
								} else {
									vals[j] = norm
								}
							}
						} else {
							vals[j] = cell
						}
					}
				}

				// capture centre_code value (if present) for audit selection later
				if pos, ok := headerPos["centre_code"]; ok && pos < len(row) {
					cc := strings.TrimSpace(row[pos])
					if cc != "" {
						centreCodes = append(centreCodes, cc)
					}
				}

				copyRows[i] = vals
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed")
				return
			}
			defer tx.Rollback(ctx)

			if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '10min'"); err != nil {
			}

			_, err = tx.CopyFrom(ctx, pgx.Identifier{"mastercostprofitcenter"}, validCols, pgx.CopyFromRows(copyRows))
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "COPY failed: "+err.Error())
				return
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}

			// Async sync + audit - pass the centre codes so we can find the newly inserted rows
			go func(userName string, centreCodes []string) {
				ctx2 := context.Background()
				tx2, err := pgxPool.Begin(ctx2)
				if err != nil {
					log.Printf("Async tx begin failed: %v", err)
					return
				}
				defer tx2.Rollback(ctx2)

				_, err = tx2.Exec(ctx2, `
					-- Set root levels (use parent_centre_code -> centre_code)
					UPDATE mastercostprofitcenter
					SET centre_level = 0, is_top_level_centre = true
					WHERE parent_centre_code IS NULL OR TRIM(parent_centre_code) = ''
					   OR parent_centre_code NOT IN (SELECT centre_code FROM mastercostprofitcenter);

					-- Build recursive levels using centre_code link
					WITH RECURSIVE cte AS (
						SELECT centre_id, centre_code, parent_centre_code, 0 lvl
						FROM mastercostprofitcenter
						WHERE parent_centre_code IS NULL OR TRIM(parent_centre_code) = ''
						UNION ALL
						SELECT c.centre_id, c.centre_code, c.parent_centre_code, p.lvl + 1
						FROM mastercostprofitcenter c
						JOIN cte p ON c.parent_centre_code = p.centre_code
					)
					UPDATE mastercostprofitcenter m
					SET centre_level = cte.lvl,
						is_top_level_centre = (cte.lvl=0)
					FROM cte WHERE m.centre_id = cte.centre_id;

					-- Relationships (store codes in relationships table)
					INSERT INTO costprofitcenterrelationships (parent_centre_code, child_centre_code, status)
					SELECT DISTINCT p.centre_code, c.centre_code, 'Active'
					FROM mastercostprofitcenter c
					JOIN mastercostprofitcenter p ON c.parent_centre_code = p.centre_code
					ON CONFLICT DO NOTHING;
				`)
				if err != nil {
					log.Printf("Hierarchy sync failed: %v", err)
				}

				// Insert audit rows for the centre_codes we just uploaded. Use centre_code to locate rows
				if len(centreCodes) > 0 {
					// dedupe centreCodes
					uniq := make(map[string]bool)
					uniqList := make([]string, 0, len(centreCodes))
					for _, c := range centreCodes {
						if c == "" {
							continue
						}
						if !uniq[c] {
							uniq[c] = true
							uniqList = append(uniqList, c)
						}
					}
					if len(uniqList) > 0 {
						if _, err := tx2.Exec(ctx2, `
						INSERT INTO auditactioncostprofitcenter(centre_id, actiontype, processing_status, requested_by, requested_at)
						SELECT centre_id, 'CREATE', 'PENDING_APPROVAL', $1, now()
						FROM mastercostprofitcenter
						WHERE centre_code = ANY($2)
						`, userName, uniqList); err != nil {
							log.Printf("Audit insert failed: %v", err)
						}
					}
				}
				tx2.Commit(ctx2)
			}(userName, centreCodes)

			batchIDs = append(batchIDs, uuid.New().String())
		}

		json.NewEncoder(w).Encode(map[string]any{"success": true, "batch_ids": batchIDs})
	}
}

func contains(arr []string, v string) bool {
	for _, s := range arr {
		if s == v {
			return true
		}
	}
	return false
}
