package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"

	// "bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

type CashFlowCategoryRequest struct {
	CategoryName       string `json:"category_name"`
	CategoryType       string `json:"category_type"`
	ParentCategoryName string `json:"parent_category_name"`
	DefaultMapping     string `json:"default_mapping"`
	CashflowNature     string `json:"cashflow_nature"`
	UsageFlag          string `json:"usage_flag"`
	Description        string `json:"description"`
	Status             string `json:"status"`
	CategoryLevel      int    `json:"category_level"`
	ERPType            string `json:"erp_type,omitempty"`
	ERPExt             string `json:"erp_ext,omitempty"`
	ERPSegment         string `json:"erp_segment,omitempty"`
	SAPFsv             string `json:"sap_fsv,omitempty"`
	SAPNode            string `json:"sap_node,omitempty"`
	SAPBukrs           string `json:"sap_bukrs,omitempty"`
	SAPNotes           string `json:"sap_notes,omitempty"`
	OracleLedger       string `json:"oracle_ledger,omitempty"`
	OracleCFCode       string `json:"oracle_cf_code,omitempty"`
	OracleCFName       string `json:"oracle_cf_name,omitempty"`
	OracleLine         string `json:"oracle_line,omitempty"`
	TallyGroup         string `json:"tally_group,omitempty"`
	TallyVoucher       string `json:"tally_voucher,omitempty"`
	TallyNotes         string `json:"tally_notes,omitempty"`
	SageSection        string `json:"sage_section,omitempty"`
	SageLine           string `json:"sage_line,omitempty"`
	SageNotes          string `json:"sage_notes,omitempty"`
}

func ifaceToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case *string:
		if t == nil {
			return ""
		}
		return *t
	case []byte:
		return string(t)
	case fmt.Stringer:
		return t.String()
	default:
		return fmt.Sprint(t)
	}
}

func ifaceToTimeString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case time.Time:
		return t.Format(constants.DateTimeFormat)
	case *time.Time:
		if t == nil {
			return ""
		}
		return t.Format(constants.DateTimeFormat)
	case string:
		return t
	case *string:
		if t == nil {
			return ""
		}
		return *t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

// buildCategoryNameToIDMap returns a map[name(lowercase trimmed)] = category_id
func buildCategoryNameToIDMap(ctx context.Context, pgxPool *pgxpool.Pool) (map[string]string, error) {
	rows, err := pgxPool.Query(ctx, `SELECT category_name, category_id FROM mastercashflowcategory`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := make(map[string]string)
	for rows.Next() {
		var name, id string
		if err := rows.Scan(&name, &id); err == nil {
			key := strings.ToLower(strings.TrimSpace(name))
			m[key] = id
		}
	}
	return m, nil
}

func ifaceToInt(v interface{}) int {
	if v == nil {
		return 0
	}
	switch t := v.(type) {
	case int:
		return t
	case int8:
		return int(t)
	case int16:
		return int(t)
	case int32:
		return int(t)
	case int64:
		return int(t)
	case uint:
		return int(t)
	case uint8:
		return int(t)
	case uint16:
		return int(t)
	case uint32:
		return int(t)
	case uint64:
		return int(t)
	case float32:
		return int(t)
	case float64:
		return int(t)
	case []byte:
		s := string(t)
		var i int
		fmt.Sscan(s, &i)
		return i
	case string:
		var i int
		fmt.Sscan(t, &i)
		return i
	default:
		return 0
	}
}

func getFileExt(filename string) string {
	return strings.ToLower(filepath.Ext(filename))
}

func parseCashFlowCategoryFile(file multipart.File, ext string) ([][]string, error) {
	if ext == ".csv" {
		r := csv.NewReader(file)
		return r.ReadAll()
	}
	if ext == ".xlsx" || ext == ".xls" {
		f, err := excelize.OpenReader(file)
		if err != nil {
			return nil, err
		}
		sheet := f.GetSheetName(0)
		rows, err := f.GetRows(sheet)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	return nil, errors.New(constants.ErrUnsupportedFileType)
}

func CreateAndSyncCashFlowCategories(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string                    `json:"user_id"`
			Categories []CashFlowCategoryRequest `json:"categories"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				createdBy = s.Name
				break
			}
		}
		if createdBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := context.Background()
		created := make([]map[string]interface{}, 0)
		nameToID := make(map[string]string)

		for _, cat := range req.Categories {
			if cat.CategoryName == "" || cat.CategoryType == "" || cat.DefaultMapping == "" || cat.CashflowNature == "" || cat.UsageFlag == "" || cat.Status == "" {
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrMissingRequiredFieldsUser, "category_name": cat.CategoryName})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxStartFailed + err.Error(), "category_name": cat.CategoryName})
				continue
			}
			categoryID := "CFC-" + strings.ToUpper(strings.ReplaceAll(uuid.New().String(), "-", ""))[:7]
			insertSQL := `INSERT INTO mastercashflowcategory (
		    category_id, category_name, category_type, parent_category_name, default_mapping, cashflow_nature, usage_flag, description, status, category_level,
					erp_type, erp_ext, erp_segment, sap_fsv, sap_node, sap_bukrs, sap_notes,
					oracle_ledger, oracle_cf_code, oracle_cf_name, oracle_line,
					tally_group, tally_voucher, tally_notes, sage_section, sage_line, sage_notes
								) VALUES ($1,$2,$3, NULLIF($4, ''), $5, $6, $7, $8, $9, $10,
			  $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)`
			_, err = tx.Exec(ctx, insertSQL,
				categoryID,
				cat.CategoryName,
				cat.CategoryType,
				cat.ParentCategoryName,
				cat.DefaultMapping,
				cat.CashflowNature,
				cat.UsageFlag,
				cat.Description,
				cat.Status,
				cat.CategoryLevel,
				cat.ERPType,
				cat.ERPExt,
				cat.ERPSegment,
				cat.SAPFsv,
				cat.SAPNode,
				cat.SAPBukrs,
				cat.SAPNotes,
				cat.OracleLedger,
				cat.OracleCFCode,
				cat.OracleCFName,
				cat.OracleLine,
				cat.TallyGroup,
				cat.TallyVoucher,
				cat.TallyNotes,
				cat.SageSection,
				cat.SageLine,
				cat.SageNotes,
			)
			if err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error(), "category_name": cat.CategoryName})
				continue
			}
			auditSQL := `INSERT INTO auditactioncashflowcategory (
				category_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, auditSQL, categoryID, nil, createdBy); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrAuditInsertFailed + err.Error(), "category_id": categoryID})
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailedCapitalized + err.Error(), "category_id": categoryID})
				continue
			}
			created = append(created, map[string]interface{}{constants.ValueSuccess: true, "category_id": categoryID, "category_name": cat.CategoryName})
			nameToID[strings.ToLower(cat.CategoryName)] = categoryID
		}
		relAdded := 0
		for _, cat := range req.Categories {
			childID := ""
			if id := nameToID[strings.ToLower(cat.CategoryName)]; id != "" {
				childID = id
			}
			if childID == "" {
				continue
			}
			parentName := strings.TrimSpace(cat.ParentCategoryName)
			if parentName == "" {
				continue
			}
			ctx := context.Background()
			var exists bool
			// relationships are now stored by names
			err := pgxPool.QueryRow(ctx, `SELECT true FROM cashflowcategoryrelationships WHERE parent_category_name=$1 AND child_category_name=$2`, parentName, cat.CategoryName).Scan(&exists)
			if err == nil && exists {
				continue
			}
			if _, err := pgxPool.Exec(ctx, `INSERT INTO cashflowcategoryrelationships (parent_category_name, child_category_name, status) VALUES ($1,$2,'Active')`, parentName, cat.CategoryName); err == nil {
				relAdded++
			}
		}
		overall := api.IsBulkSuccess(created)
		api.RespondWithPayload(w, overall, "", map[string]interface{}{
			"created":             created,
			"relationships_added": relAdded,
		})
	}
}

func GetCashFlowCategoryHierarchyPGX(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		query := `
			SELECT m.category_id, m.category_name, m.category_type, m.parent_category_name,
				   m.default_mapping, m.cashflow_nature, m.usage_flag, m.description, m.status,
				   m.old_category_name, m.old_category_type, m.old_parent_category_name,
				   m.old_default_mapping, m.old_cashflow_nature, m.old_usage_flag, m.old_description, m.old_status,
			       m.is_top_level_category, m.is_deleted, m.category_level, m.old_category_level,
			       m.erp_type, m.erp_ext, m.erp_segment, m.old_erp_type, m.old_erp_ext, m.old_erp_segment,
			       m.sap_fsv, m.sap_node, m.sap_bukrs, m.sap_notes, m.old_sap_fsv, m.old_sap_node, m.old_sap_bukrs, m.old_sap_notes,
			       m.oracle_ledger, m.oracle_cf_code, m.oracle_cf_name, m.oracle_line, m.old_oracle_ledger, m.old_oracle_cf_code, m.old_oracle_cf_name, m.old_oracle_line,
			       m.tally_group, m.tally_voucher, m.tally_notes, m.old_tally_group, m.old_tally_voucher, m.old_tally_notes,
			       m.sage_section, m.sage_line, m.sage_notes, m.old_sage_section, m.old_sage_line, m.old_sage_notes,
			       a.processing_status, a.requested_by, a.requested_at, a.actiontype, a.action_id,
			       a.checker_by, a.checker_at, a.checker_comment, a.reason
			FROM mastercashflowcategory m
			LEFT JOIN LATERAL (
				SELECT processing_status, requested_by, requested_at, actiontype, action_id,
				       checker_by, checker_at, checker_comment, reason
				FROM auditactioncashflowcategory a
				WHERE a.category_id = m.category_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE;
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		entityMap := map[string]map[string]interface{}{}
		typeIDs := []string{}
		hideIds := map[string]bool{}

		for rows.Next() {
			var (
				categoryID, actionIDI                                                              string
				categoryNameI, categoryTypeI                                                       interface{}
				parentCategoryNameI                                                                *string
				defaultMappingI, cashflowNatureI, usageFlagI, descriptionI, statusI                interface{}
				oldCategoryNameI, oldCategoryTypeI                                                 interface{}
				oldParentCategoryNameI                                                             *string
				oldDefaultMappingI, oldCashflowNatureI, oldUsageFlagI, oldDescriptionI, oldStatusI interface{}
				isTopLevelCategory, isDeleted                                                      bool
				categoryLevelI, oldCategoryLevelI, processingStatusI                               interface{}
				requestedByI, requestedAtI, actionTypeI                                            interface{}
				checkerByI, checkerAtI, checkerCommentI, reasonI                                   interface{}
				erpTypeI, erpExtI, erpSegmentI                                                     interface{}
				oldErpTypeI, oldErpExtI, oldErpSegmentI                                            interface{}
				sapFsvI, sapNodeI, sapBukrsI, sapNotesI                                            interface{}
				oldSapFsvI, oldSapNodeI, oldSapBukrsI, oldSapNotesI                                interface{}
				oracleLedgerI, oracleCFCodeI, oracleCFNameI, oracleLineI                           interface{}
				oldOracleLedgerI, oldOracleCFCodeI, oldOracleCFNameI, oldOracleLineI               interface{}
				tallyGroupI, tallyVoucherI, tallyNotesI                                            interface{}
				oldTallyGroupI, oldTallyVoucherI, oldTallyNotesI                                   interface{}
				sageSectionI, sageLineI, sageNotesI                                                interface{}
				oldSageSectionI, oldSageLineI, oldSageNotesI                                       interface{}
			)

			if err := rows.Scan(
				&categoryID, &categoryNameI, &categoryTypeI, &parentCategoryNameI,
				&defaultMappingI, &cashflowNatureI, &usageFlagI, &descriptionI, &statusI,
				&oldCategoryNameI, &oldCategoryTypeI, &oldParentCategoryNameI,
				&oldDefaultMappingI, &oldCashflowNatureI, &oldUsageFlagI,
				&oldDescriptionI, &oldStatusI,
				&isTopLevelCategory, &isDeleted, &categoryLevelI, &oldCategoryLevelI,
				&erpTypeI, &erpExtI, &erpSegmentI, &oldErpTypeI, &oldErpExtI, &oldErpSegmentI,
				&sapFsvI, &sapNodeI, &sapBukrsI, &sapNotesI, &oldSapFsvI, &oldSapNodeI, &oldSapBukrsI, &oldSapNotesI,
				&oracleLedgerI, &oracleCFCodeI, &oracleCFNameI, &oracleLineI, &oldOracleLedgerI, &oldOracleCFCodeI, &oldOracleCFNameI, &oldOracleLineI,
				&tallyGroupI, &tallyVoucherI, &tallyNotesI, &oldTallyGroupI, &oldTallyVoucherI, &oldTallyNotesI,
				&sageSectionI, &sageLineI, &sageNotesI, &oldSageSectionI, &oldSageLineI, &oldSageNotesI,
				&processingStatusI, &requestedByI, &requestedAtI, &actionTypeI, &actionIDI,
				&checkerByI, &checkerAtI, &checkerCommentI, &reasonI,
			); err != nil {
				continue
			}

			entityMap[categoryID] = map[string]interface{}{
				"id":   categoryID,
				"name": ifaceToString(categoryNameI),
				"data": map[string]interface{}{
					"category_id":              categoryID,
					"category_name":            ifaceToString(categoryNameI),
					"category_type":            ifaceToString(categoryTypeI),
					"parent_category_name":     ifaceToString(parentCategoryNameI),
					"default_mapping":          ifaceToString(defaultMappingI),
					"cashflow_nature":          ifaceToString(cashflowNatureI),
					"usage_flag":               ifaceToString(usageFlagI),
					"description":              ifaceToString(descriptionI),
					constants.KeyStatus:        ifaceToString(statusI),
					"old_category_name":        ifaceToString(oldCategoryNameI),
					"old_category_type":        ifaceToString(oldCategoryTypeI),
					"old_parent_category_name": ifaceToString(oldParentCategoryNameI),
					"old_default_mapping":      ifaceToString(oldDefaultMappingI),
					"old_cashflow_nature":      ifaceToString(oldCashflowNatureI),
					"old_usage_flag":           ifaceToString(oldUsageFlagI),
					"old_description":          ifaceToString(oldDescriptionI),
					"old_status":               ifaceToString(oldStatusI),
					"is_top_level_category":    isTopLevelCategory,
					"is_deleted":               isDeleted,
					"category_level":           ifaceToInt(categoryLevelI),
					"old_category_level":       ifaceToInt(oldCategoryLevelI),
					"erp_type":                 ifaceToString(erpTypeI),
					"erp_ext":                  ifaceToString(erpExtI),
					"erp_segment":              ifaceToString(erpSegmentI),
					"old_erp_type":             ifaceToString(oldErpTypeI),
					"old_erp_ext":              ifaceToString(oldErpExtI),
					"old_erp_segment":          ifaceToString(oldErpSegmentI),
					"sap_fsv":                  ifaceToString(sapFsvI),
					"sap_node":                 ifaceToString(sapNodeI),
					"sap_bukrs":                ifaceToString(sapBukrsI),
					"sap_notes":                ifaceToString(sapNotesI),
					"old_sap_fsv":              ifaceToString(oldSapFsvI),
					"old_sap_node":             ifaceToString(oldSapNodeI),
					"old_sap_bukrs":            ifaceToString(oldSapBukrsI),
					"old_sap_notes":            ifaceToString(oldSapNotesI),
					"oracle_ledger":            ifaceToString(oracleLedgerI),
					"oracle_cf_code":           ifaceToString(oracleCFCodeI),
					"oracle_cf_name":           ifaceToString(oracleCFNameI),
					"oracle_line":              ifaceToString(oracleLineI),
					"old_oracle_ledger":        ifaceToString(oldOracleLedgerI),
					"old_oracle_cf_code":       ifaceToString(oldOracleCFCodeI),
					"old_oracle_cf_name":       ifaceToString(oldOracleCFNameI),
					"old_oracle_line":          ifaceToString(oldOracleLineI),
					"tally_group":              ifaceToString(tallyGroupI),
					"tally_voucher":            ifaceToString(tallyVoucherI),
					"tally_notes":              ifaceToString(tallyNotesI),
					"old_tally_group":          ifaceToString(oldTallyGroupI),
					"old_tally_voucher":        ifaceToString(oldTallyVoucherI),
					"old_tally_notes":          ifaceToString(oldTallyNotesI),
					"sage_section":             ifaceToString(sageSectionI),
					"sage_line":                ifaceToString(sageLineI),
					"sage_notes":               ifaceToString(sageNotesI),
					"old_sage_section":         ifaceToString(oldSageSectionI),
					"old_sage_line":            ifaceToString(oldSageLineI),
					"old_sage_notes":           ifaceToString(oldSageNotesI),
					"processing_status":        ifaceToString(processingStatusI),
					"requested_by":             ifaceToString(requestedByI),
					"requested_at":             ifaceToTimeString(requestedAtI),
					"created_by":               "", "created_at": "", "edited_by": "", "edited_at": "", "deleted_by": "", "deleted_at": "",
					"action_type":     ifaceToString(actionTypeI),
					"action_id":       ifaceToString(actionIDI),
					"checker_by":      ifaceToString(checkerByI),
					"checker_at":      ifaceToTimeString(checkerAtI),
					"checker_comment": ifaceToString(checkerCommentI),
					"reason":          ifaceToString(reasonI),
				},
				"children": []interface{}{},
			}

			typeIDs = append(typeIDs, categoryID)

			if isDeleted && strings.ToUpper(ifaceToString(processingStatusI)) == "APPROVED" {
				hideIds[categoryID] = true
			}
		}

		if len(typeIDs) > 0 {
			auditQuery := `
				SELECT category_id, actiontype, requested_by, requested_at
				FROM auditactioncashflowcategory
				WHERE category_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE')
				ORDER BY requested_at DESC;
			`
			auditRows, err := pgxPool.Query(ctx, auditQuery, typeIDs)
			if err == nil {
				defer auditRows.Close()
				auditMap := make(map[string]map[string]string)
				for auditRows.Next() {
					var cid, atype string
					var rbyI, ratI interface{}
					if err := auditRows.Scan(&cid, &atype, &rbyI, &ratI); err == nil {
						if auditMap[cid] == nil {
							auditMap[cid] = map[string]string{}
						}
						switch atype {
						case "CREATE":
							if auditMap[cid]["created_by"] == "" {
								auditMap[cid]["created_by"] = ifaceToString(rbyI)
								auditMap[cid]["created_at"] = ifaceToTimeString(ratI)
							}
						case "EDIT":
							if auditMap[cid]["edited_by"] == "" {
								auditMap[cid]["edited_by"] = ifaceToString(rbyI)
								auditMap[cid]["edited_at"] = ifaceToTimeString(ratI)
							}
						case "DELETE":
							if auditMap[cid]["deleted_by"] == "" {
								auditMap[cid]["deleted_by"] = ifaceToString(rbyI)
								auditMap[cid]["deleted_at"] = ifaceToTimeString(ratI)
							}
						}
					}
				}
				for cid, info := range auditMap {
					if entity, ok := entityMap[cid]; ok {
						data := entity["data"].(map[string]interface{})
						for k, v := range info {
							data[k] = v
						}
					}
				}
			}
		}

		// relationships are stored by names; build name->id map and map relationships to ids
		nameToID, _ := buildCategoryNameToIDMap(ctx, pgxPool)
		relRows, err := pgxPool.Query(ctx, "SELECT parent_category_name, child_category_name FROM cashflowcategoryrelationships")
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()

		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentName, childName string
			if err := relRows.Scan(&parentName, &childName); err == nil {
				pid := nameToID[strings.ToLower(strings.TrimSpace(parentName))]
				cid := nameToID[strings.ToLower(strings.TrimSpace(childName))]
				if pid != "" && cid != "" {
					parentMap[pid] = append(parentMap[pid], cid)
				}
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
						entityMap[parentID]["children"] = append(
							entityMap[parentID]["children"].([]interface{}),
							entityMap[childID],
						)
					}
				}
			}
		}
		childIDs := map[string]bool{}
		for _, children := range parentMap {
			for _, cid := range children {
				if entityMap[cid] != nil {
					childIDs[cid] = true
				}
			}
		}
		rowsOut := []map[string]interface{}{}
		for _, id := range typeIDs {
			if entity, ok := entityMap[id]; ok {
				if !childIDs[id] {
					rowsOut = append(rowsOut, entity)
				}
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(rowsOut)
	}
}

func FindParentCashFlowCategoryAtLevel(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		var req struct {
			UserID string `json:"user_id"`
			Level  int    `json:"level"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing or invalid user_id/level")
			return
		}

		validUser := false
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				validUser = true
				break
			}
		}
		if !validUser {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		parentLevel := req.Level - 1
		query := `
			SELECT m.category_name, m.category_id
			FROM mastercashflowcategory m
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactioncashflowcategory a
				WHERE a.category_id = m.category_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE m.category_level = $1
			  AND (m.is_deleted = false OR m.is_deleted IS NULL)
			  AND LOWER(m.status) = 'active'
			  AND UPPER(COALESCE(a.processing_status, '')) = 'APPROVED'
		`

		rows, err := pgxPool.Query(context.Background(), query, parentLevel)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			var name, id string
			if err := rows.Scan(&name, &id); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			results = append(results, map[string]interface{}{"name": name, "id": id})
		}

		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", results)
	}
}
func GetCashFlowCategoryNamesWithID(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		valid := false
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := context.Background()
		rows, err := pgxPool.Query(ctx, `
			SELECT m.category_id, m.category_name, m.category_type, m.is_deleted, a.processing_status
			FROM mastercashflowcategory m
			LEFT JOIN LATERAL (
				SELECT processing_status FROM auditactioncashflowcategory a WHERE a.category_id = m.category_id ORDER BY requested_at DESC LIMIT 1
			) a ON TRUE
		`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var out []map[string]interface{}
		for rows.Next() {
			var id string
			var name, ctype interface{}
			var isDeleted bool
			var processingStatus interface{}
			if err := rows.Scan(&id, &name, &ctype, &isDeleted, &processingStatus); err == nil {
				if !isDeleted && strings.ToUpper(ifaceToString(processingStatus)) == "APPROVED" {
					out = append(out, map[string]interface{}{"category_id": ifaceToString(id), "category_name": ifaceToString(name), "category_type": ifaceToString(ctype)})
				}
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "categories": out})
	}
}
func UpdateCashFlowCategoryBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string `json:"user_id"`
			Categories []struct {
				CategoryID string                 `json:"category_id"`
				Fields     map[string]interface{} `json:"fields"`
				Reason     string                 `json:"reason"`
			} `json:"categories"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		updatedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				updatedBy = s.Name
				break
			}
		}
		if updatedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		var results []map[string]interface{}
		var relationshipsAdded []map[string]interface{}

		ctx := context.Background()
		for _, cat := range req.Categories {
			if cat.CategoryID == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Missing category_id"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxStartFailed + err.Error(), "category_id": cat.CategoryID})
				continue
			}
			committed := false

			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
				}()
				var (
					existingCategoryNameI                                                                    interface{}
					existingCategoryTypeI                                                                    interface{}
					existingParentCategoryNameI                                                              *string
					existingDefaultMappingI                                                                  interface{}
					existingCashflowNatureI                                                                  interface{}
					existingUsageFlagI                                                                       interface{}
					existingDescriptionI                                                                     interface{}
					existingStatusI                                                                          interface{}
					existingCategoryLevelI                                                                   interface{}
					existingIsTopLevel                                                                       bool
					existingIsDeleted                                                                        bool
					existingErpTypeI, existingErpExtI, existingErpSegmentI                                   interface{}
					existingSapFsvI, existingSapNodeI, existingSapBukrsI, existingSapNotesI                  interface{}
					existingOracleLedgerI, existingOracleCFCodeI, existingOracleCFNameI, existingOracleLineI interface{}
					existingTallyGroupI, existingTallyVoucherI, existingTallyNotesI                          interface{}
					existingSageSectionI, existingSageLineI, existingSageNotesI                              interface{}
				)
				sel := `SELECT category_name, category_type, parent_category_name, default_mapping, cashflow_nature, usage_flag, description, status, is_top_level_category, is_deleted, category_level,
							   erp_type, erp_ext, erp_segment, sap_fsv, sap_node, sap_bukrs, sap_notes,
							   oracle_ledger, oracle_cf_code, oracle_cf_name, oracle_line,
							   tally_group, tally_voucher, tally_notes, sage_section, sage_line, sage_notes
						FROM mastercashflowcategory WHERE category_id=$1 FOR UPDATE`
				if err := tx.QueryRow(ctx, sel, cat.CategoryID).Scan(&existingCategoryNameI, &existingCategoryTypeI, &existingParentCategoryNameI, &existingDefaultMappingI, &existingCashflowNatureI, &existingUsageFlagI, &existingDescriptionI, &existingStatusI, &existingIsTopLevel, &existingIsDeleted, &existingCategoryLevelI,
					&existingErpTypeI, &existingErpExtI, &existingErpSegmentI, &existingSapFsvI, &existingSapNodeI, &existingSapBukrsI, &existingSapNotesI,
					&existingOracleLedgerI, &existingOracleCFCodeI, &existingOracleCFNameI, &existingOracleLineI,
					&existingTallyGroupI, &existingTallyVoucherI, &existingTallyNotesI, &existingSageSectionI, &existingSageLineI, &existingSageNotesI); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Failed to fetch existing category: " + err.Error(), "category_id": cat.CategoryID})
					return
				}
				var sets []string
				var args []interface{}
				pos := 1
				parentProvided := ""
				for k, v := range cat.Fields {
					switch k {
					case "category_name":
						sets = append(sets, fmt.Sprintf("category_name=$%d, old_category_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCategoryNameI))
						pos += 2
					case "category_type":
						sets = append(sets, fmt.Sprintf("category_type=$%d, old_category_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCategoryTypeI))
						pos += 2
					case "parent_category_name":
						newParent := fmt.Sprint(v)
						parentProvided = newParent
						sets = append(sets, fmt.Sprintf("parent_category_name=$%d, old_parent_category_name=$%d", pos, pos+1))
						oldParentVal := ""
						if existingParentCategoryNameI != nil {
							oldParentVal = *existingParentCategoryNameI
						}
						args = append(args, newParent, oldParentVal)
						pos += 2
					case "default_mapping":
						sets = append(sets, fmt.Sprintf("default_mapping=$%d, old_default_mapping=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingDefaultMappingI))
						pos += 2
					case "cashflow_nature":
						sets = append(sets, fmt.Sprintf("cashflow_nature=$%d, old_cashflow_nature=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCashflowNatureI))
						pos += 2
					case "usage_flag":
						sets = append(sets, fmt.Sprintf("usage_flag=$%d, old_usage_flag=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingUsageFlagI))
						pos += 2
					case "description":
						sets = append(sets, fmt.Sprintf("description=$%d, old_description=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingDescriptionI))
						pos += 2
					case constants.KeyStatus:
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingStatusI))
						pos += 2
					case "category_level":
						var newLevel int
						switch t := v.(type) {
						case float64:
							newLevel = int(t)
						case int:
							newLevel = t
						case string:
							var tmp int
							fmt.Sscan(t, &tmp)
							newLevel = tmp
						default:
							newLevel = 0
						}
						sets = append(sets, fmt.Sprintf("category_level=$%d, old_category_level=$%d", pos, pos+1))
						args = append(args, newLevel, ifaceToInt(existingCategoryLevelI))
						pos += 2
					case "is_top_level_category":
						var newBool bool
						switch t := v.(type) {
						case bool:
							newBool = t
						case string:
							if strings.ToLower(t) == "true" {
								newBool = true
							} else {
								newBool = false
							}
						default:
							newBool = false
						}
						sets = append(sets, fmt.Sprintf("is_top_level_category=$%d", pos))
						args = append(args, newBool)
						pos++
					case "is_deleted":
						var newBool bool
						switch t := v.(type) {
						case bool:
							newBool = t
						case string:
							if strings.ToLower(t) == "true" {
								newBool = true
							} else {
								newBool = false
							}
						default:
							newBool = false
						}
						sets = append(sets, fmt.Sprintf("is_deleted=$%d", pos))
						args = append(args, newBool)
						pos++
					case "erp_type":
						sets = append(sets, fmt.Sprintf("erp_type=$%d, old_erp_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingErpTypeI))
						pos += 2
					case "erp_ext":
						sets = append(sets, fmt.Sprintf("erp_ext=$%d, old_erp_ext=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingErpExtI))
						pos += 2
					case "erp_segment":
						sets = append(sets, fmt.Sprintf("erp_segment=$%d, old_erp_segment=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingErpSegmentI))
						pos += 2
					case "sap_fsv":
						sets = append(sets, fmt.Sprintf("sap_fsv=$%d, old_sap_fsv=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapFsvI))
						pos += 2
					case "sap_node":
						sets = append(sets, fmt.Sprintf("sap_node=$%d, old_sap_node=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapNodeI))
						pos += 2
					case "sap_bukrs":
						sets = append(sets, fmt.Sprintf("sap_bukrs=$%d, old_sap_bukrs=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapBukrsI))
						pos += 2
					case "sap_notes":
						sets = append(sets, fmt.Sprintf("sap_notes=$%d, old_sap_notes=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSapNotesI))
						pos += 2
					case "oracle_ledger":
						sets = append(sets, fmt.Sprintf("oracle_ledger=$%d, old_oracle_ledger=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleLedgerI))
						pos += 2
					case "oracle_cf_code":
						sets = append(sets, fmt.Sprintf("oracle_cf_code=$%d, old_oracle_cf_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleCFCodeI))
						pos += 2
					case "oracle_cf_name":
						sets = append(sets, fmt.Sprintf("oracle_cf_name=$%d, old_oracle_cf_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleCFNameI))
						pos += 2
					case "oracle_line":
						sets = append(sets, fmt.Sprintf("oracle_line=$%d, old_oracle_line=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingOracleLineI))
						pos += 2
					case "tally_group":
						sets = append(sets, fmt.Sprintf("tally_group=$%d, old_tally_group=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTallyGroupI))
						pos += 2
					case "tally_voucher":
						sets = append(sets, fmt.Sprintf("tally_voucher=$%d, old_tally_voucher=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTallyVoucherI))
						pos += 2
					case "tally_notes":
						sets = append(sets, fmt.Sprintf("tally_notes=$%d, old_tally_notes=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingTallyNotesI))
						pos += 2
					case "sage_section":
						sets = append(sets, fmt.Sprintf("sage_section=$%d, old_sage_section=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSageSectionI))
						pos += 2
					case "sage_line":
						sets = append(sets, fmt.Sprintf("sage_line=$%d, old_sage_line=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSageLineI))
						pos += 2
					case "sage_notes":
						sets = append(sets, fmt.Sprintf("sage_notes=$%d, old_sage_notes=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSageNotesI))
						pos += 2
					default:
					}
				}

				var updatedCategoryID string
				if len(sets) > 0 {
					q := "UPDATE mastercashflowcategory SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE category_id=$%d RETURNING category_id", pos)
					args = append(args, cat.CategoryID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedCategoryID); err != nil {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error(), "category_id": cat.CategoryID})
						return
					}
				} else {
					updatedCategoryID = cat.CategoryID
				}
				auditQuery := `INSERT INTO auditactioncashflowcategory (
					category_id, actiontype, processing_status, reason, requested_by, requested_at
				) VALUES ($1, $2, $3, $4, $5, now())`
				if _, err := tx.Exec(ctx, auditQuery, updatedCategoryID, "EDIT", "PENDING_EDIT_APPROVAL", cat.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Category updated but audit log failed: " + err.Error(), "category_id": updatedCategoryID})
					return
				}
				if parentProvided != "" {
					var exists bool
					err := tx.QueryRow(ctx, `SELECT true FROM cashflowcategoryrelationships WHERE parent_category_name=$1 AND child_category_name=$2`, parentProvided, ifaceToString(existingCategoryNameI)).Scan(&exists)
					// Insert only when the query returned an error or the relationship does not exist
					if err != nil || !exists {
						if _, err := tx.Exec(ctx, `INSERT INTO cashflowcategoryrelationships (parent_category_name, child_category_name, status) VALUES ($1,$2,'Active')`, parentProvided, ifaceToString(existingCategoryNameI)); err == nil {
							relationshipsAdded = append(relationshipsAdded, map[string]interface{}{constants.ValueSuccess: true, "parent_category_name": parentProvided, "child_category_name": ifaceToString(existingCategoryNameI)})
						}
					}
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Transaction commit failed: " + err.Error(), "category_id": updatedCategoryID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "category_id": updatedCategoryID})
			}()
		}

		overall := api.IsBulkSuccess(results)
		api.RespondWithPayload(w, overall, "", map[string]interface{}{
			"results":            results,
			"relationshipsAdded": len(relationshipsAdded),
			"details":            relationshipsAdded,
		})
	}
}
func DeleteCashFlowCategory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"`
			IDs    []string `json:"ids"`
			Reason string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		if req.UserID == "" || len(req.IDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Missing user_id or ids")
			return
		}

		requestedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := context.Background()

		nameToID, _ := buildCategoryNameToIDMap(ctx, pgxPool)
		relRows, err := pgxPool.Query(ctx, `SELECT parent_category_name, child_category_name FROM cashflowcategoryrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentName, childName string
			if err := relRows.Scan(&parentName, &childName); err == nil {
				pid := nameToID[strings.ToLower(strings.TrimSpace(parentName))]
				cid := nameToID[strings.ToLower(strings.TrimSpace(childName))]
				if pid != "" && cid != "" {
					parentMap[pid] = append(parentMap[pid], cid)
				}
			}
		}
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
			result := []string{}
			for id := range all {
				result = append(result, id)
			}
			return result
		}

		allToDelete := getAllDescendants(req.IDs)
		if len(allToDelete) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No category found to delete")
			return
		}
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()
		rows, err := tx.Query(ctx, `UPDATE mastercashflowcategory SET is_deleted = true WHERE category_id = ANY($1) RETURNING category_id`, allToDelete)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		updated := []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				updated = append(updated, id)
			}
		}

		if len(updated) == 0 {
			tx.Rollback(ctx)
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoRowsUpdated)
			return
		}
		for _, cid := range updated {
			if _, err := tx.Exec(ctx, `INSERT INTO auditactioncashflowcategory (category_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now())`, cid, req.Reason, requestedBy); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}
		committed = true

		api.RespondWithPayload(w, true, "", map[string]interface{}{"updated": updated})
	}
}
func BulkRejectCashFlowCategoryActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CategoryIDs []string `json:"category_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		checkerBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := context.Background()
		nameToID, _ := buildCategoryNameToIDMap(ctx, pgxPool)
		relRows, err := pgxPool.Query(ctx, `SELECT parent_category_name, child_category_name FROM cashflowcategoryrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentName, childName string
			if err := relRows.Scan(&parentName, &childName); err == nil {
				pid := nameToID[strings.ToLower(strings.TrimSpace(parentName))]
				cid := nameToID[strings.ToLower(strings.TrimSpace(childName))]
				if pid != "" && cid != "" {
					parentMap[pid] = append(parentMap[pid], cid)
				}
			}
		}
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

		allToReject := getAllDescendants(req.CategoryIDs)
		if len(allToReject) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No categories found to reject")
			return
		}
		query := `UPDATE auditactioncashflowcategory SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE category_id = ANY($3) RETURNING action_id, category_id`
		rows, err := pgxPool.Query(ctx, query, checkerBy, req.Comment, allToReject)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var updated []map[string]interface{}
		for rows.Next() {
			var actionID, categoryID string
			if err := rows.Scan(&actionID, &categoryID); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": actionID, "category_id": categoryID})
			}
		}

		success := len(updated) > 0
		if !success {
			api.RespondWithPayload(w, false, constants.ErrNoRowsUpdated, map[string]interface{}{"updated": updated})
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"updated": updated})
	}
}

func BulkApproveCashFlowCategoryActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CategoryIDs []string `json:"category_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		checkerBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := context.Background()

		nameToID, _ := buildCategoryNameToIDMap(ctx, pgxPool)
		relRows, err := pgxPool.Query(ctx, `SELECT parent_category_name, child_category_name FROM cashflowcategoryrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentName, childName string
			if err := relRows.Scan(&parentName, &childName); err == nil {
				pid := nameToID[strings.ToLower(strings.TrimSpace(parentName))]
				cid := nameToID[strings.ToLower(strings.TrimSpace(childName))]
				if pid != "" && cid != "" {
					parentMap[pid] = append(parentMap[pid], cid)
				}
			}
		}

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

		allToApprove := getAllDescendants(req.CategoryIDs)
		if len(allToApprove) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No categories found to approve")
			return
		}

		query := `UPDATE auditactioncashflowcategory SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE category_id = ANY($3) RETURNING action_id, category_id, actiontype`
		rows, err := pgxPool.Query(ctx, query, checkerBy, req.Comment, allToApprove)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var updated []map[string]interface{}
		for rows.Next() {
			var actionID, categoryID, actionType string
			if err := rows.Scan(&actionID, &categoryID, &actionType); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": actionID, "category_id": categoryID, "action_type": actionType})
			}
		}

		success := len(updated) > 0
		if !success {
			api.RespondWithPayload(w, false, constants.ErrNoRowsUpdated, map[string]interface{}{"updated": updated})
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"updated": updated})
	}
}

func UploadCashFlowCategory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := ""
		if r.Header.Get(constants.ContentTypeText) == constants.ContentTypeJSON {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in body")
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue(constants.KeyUserID)
			if userID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in form")
				return
			}
		}

		userName := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseMultipartForm)
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFilesUploaded)
			return
		}
		batchIDs := make([]string, 0, len(files))
		for _, fileHeader := range files {
			file, err := fileHeader.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file: "+fileHeader.Filename)
				return
			}
			ext := getFileExt(fileHeader.Filename)
			records, err := parseCashFlowCategoryFile(file, ext)
			file.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file: "+fileHeader.Filename)
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
			columns := append([]string{"upload_batch_id"}, headerRow...)

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed+err.Error())
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()

			_, err = tx.CopyFrom(
				ctx,
				pgx.Identifier{"input_cashflow_category"},
				columns,
				pgx.CopyFromRows(copyRows),
			)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_cashflow_category`)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Mapping error")
				return
			}
			mapping := make(map[string]string)
			for mapRows.Next() {
				var src, tgt string
				if err := mapRows.Scan(&src, &tgt); err == nil {
					mapping[src] = tgt
				}
			}
			mapRows.Close()
			var srcCols []string
			var tgtCols []string
			for _, h := range headerRow {
				if tgt, ok := mapping[h]; ok {
					srcCols = append(srcCols, h)
					tgtCols = append(tgtCols, tgt)
				} else {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf(constants.ErrNoMappingForSourceColumn, h))
					return
				}
			}
			tgtColsStr := strings.Join(tgtCols, ", ")
			var selectExprs []string
			for i, src := range srcCols {
				tgt := tgtCols[i]
				if strings.ToLower(tgt) == "parent_category_id" {
					selectExprs = append(selectExprs, fmt.Sprintf("CASE WHEN COALESCE(s.%s::text, '') = '' THEN NULL ELSE s.%s END AS %s", src, src, tgt))
				} else {
					selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", src, tgt))
				}
			}
			srcColsStr := strings.Join(selectExprs, ", ")
			insertSQL := fmt.Sprintf(`
				INSERT INTO mastercashflowcategory (%s)
				SELECT %s
				FROM input_cashflow_category s
				WHERE s.upload_batch_id = $1
				RETURNING category_id
			`, tgtColsStr, srcColsStr)
			rows, err := tx.Query(ctx, insertSQL, batchID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Final insert error: "+err.Error())
				return
			}
			var newCategoryIDs []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err == nil {
					newCategoryIDs = append(newCategoryIDs, id)
				}
			}
			rows.Close()
			if len(newCategoryIDs) > 0 {
				auditSQL := `
					INSERT INTO auditactioncashflowcategory (
						category_id, actiontype, processing_status, reason, requested_by, requested_at
					)
					SELECT category_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now()
					FROM mastercashflowcategory
					WHERE category_id = ANY($2)
				`
				_, err = tx.Exec(ctx, auditSQL, userName, newCategoryIDs)
				if err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit actions: "+err.Error())
					return
				}
			}
			if len(newCategoryIDs) > 0 {
				relSQL := `
					INSERT INTO cashflowcategoryrelationships (parent_category_id, child_category_id, status)
					SELECT parent_category_id, category_id, 'Active'
					FROM mastercashflowcategory
					WHERE category_id = ANY($1) AND parent_category_id IS NOT NULL
				`
				_, err = tx.Exec(ctx, relSQL, newCategoryIDs)
				if err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert relationships: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
				return
			}
			committed = true
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"batch_ids": batchIDs,
			"message":   "All cash flow categories uploaded, mapped, synced, and audited",
		})
	}
}

func UploadCashFlowCategorySimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		startOverall := time.Now()
		timings := make([]map[string]interface{}, 0)

		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		if err := r.ParseMultipartForm(64 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse multipart form: "+err.Error())
			return
		}
		file, fh, err := r.FormFile("file")
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "No file uploaded: "+err.Error())
			return
		}
		defer file.Close()

		records, err := parseCashFlowCategoryFile(file, getFileExt(fh.Filename))
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "File parsing failed: "+err.Error())
			return
		}
		if len(records) < 2 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidOrEmptyFile)
			return
		}
		header := records[0]
		dataRows := records[1:]

		for i, h := range header {
			header[i] = strings.ToLower(strings.TrimSpace(h))
		}

		allowed := map[string]bool{
			"category_name": true, "category_type": true, "parent_category_name": true,
			"default_mapping": true, "cashflow_nature": true, "usage_flag": true,
			"description": true, constants.KeyStatus: true, "is_deleted": true,
			"erp_type": true, "erp_ext": true, "erp_segment": true,
			"sap_fsv": true, "sap_node": true, "sap_bukrs": true, "sap_notes": true,
			"oracle_ledger": true, "oracle_cf_code": true, "oracle_cf_name": true, "oracle_line": true,
			"tally_group": true, "tally_voucher": true, "tally_notes": true,
			"sage_section": true, "sage_line": true, "sage_notes": true,
		}

		copyCols := make([]string, 0, len(header))
		headerPos := map[string]int{}
		for i, h := range header {
			headerPos[h] = i
			if allowed[h] {
				copyCols = append(copyCols, h)
			}
		}
		if !(contains(copyCols, "category_name") && contains(copyCols, "category_type")) {
			api.RespondWithError(w, http.StatusBadRequest, "CSV must include category_name and category_type")
			return
		}
		copyRows := make([][]interface{}, len(dataRows))
		rowCount := len(dataRows)
		fileStart := time.Now()
		for i, row := range dataRows {
			vals := make([]interface{}, len(copyCols))
			for j, col := range copyCols {
				if pos, ok := headerPos[col]; ok && pos < len(row) {
					cell := strings.TrimSpace(row[pos])
					if cell == "" {
						vals[j] = nil
					} else {
						vals[j] = cell
					}
				}
			}
			copyRows[i] = vals
		}
		readDur := time.Since(fileStart)
		timings = append(timings, map[string]interface{}{"phase": "read_csv", "rows": rowCount, "ms": readDur.Milliseconds()})
		log.Printf("[UploadCashFlowCategorySimple] read rows=%d elapsed=%v file=%s", rowCount, readDur, fh.Filename)

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
			return
		}
		defer func() {
			if tx != nil {
				_ = tx.Rollback(ctx)
			}
		}()

		tx.Exec(ctx, "SET LOCAL synchronous_commit TO OFF")
		tx.Exec(ctx, "SET LOCAL statement_timeout = '5min'")
		_, _ = tx.Exec(ctx, `DROP TABLE IF EXISTS tmp_mcc;`)

		_, err = tx.Exec(ctx, `
    CREATE TEMP TABLE tmp_mcc (LIKE mastercashflowcategory INCLUDING DEFAULTS) ON COMMIT DROP
`)

		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Temp table failed: "+err.Error())
			return
		}
		copyStart := time.Now()
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"tmp_mcc"}, copyCols, pgx.CopyFromRows(copyRows))
		copyDur := time.Since(copyStart)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "COPY failed: "+err.Error())
			return
		}
		timings = append(timings, map[string]interface{}{"phase": "copy_to_tmp", "rows": rowCount, "ms": copyDur.Milliseconds()})
		log.Printf("[UploadCashFlowCategorySimple] COPY rows=%d elapsed=%v file=%s", rowCount, copyDur, fh.Filename)

		_, _ = tx.Exec(ctx, `CREATE INDEX ON tmp_mcc (category_name)`)

		insertSQL := `
INSERT INTO mastercashflowcategory (
	category_name, category_type, parent_category_name, default_mapping,
	cashflow_nature, usage_flag, description, status, is_deleted,
	erp_type, erp_ext, erp_segment, sap_fsv, sap_node, sap_bukrs, sap_notes,
	oracle_ledger, oracle_cf_code, oracle_cf_name, oracle_line,
	tally_group, tally_voucher, tally_notes,
	sage_section, sage_line, sage_notes
)
SELECT
	t.category_name, t.category_type, t.parent_category_name, t.default_mapping,
	t.cashflow_nature, t.usage_flag, t.description, t.status, t.is_deleted,
	t.erp_type, t.erp_ext, t.erp_segment, t.sap_fsv, t.sap_node, t.sap_bukrs, t.sap_notes,
	t.oracle_ledger, t.oracle_cf_code, t.oracle_cf_name, t.oracle_line,
	t.tally_group, t.tally_voucher, t.tally_notes,
	t.sage_section, t.sage_line, t.sage_notes
FROM tmp_mcc t
LEFT JOIN mastercashflowcategory m ON m.category_name = t.category_name
WHERE m.category_name IS NULL;
`
		insertStart := time.Now()
		if _, err := tx.Exec(ctx, insertSQL); err != nil {
			api.RespondWithError(w, 500, "Insert failed: "+err.Error())
			return
		}
		insertDur := time.Since(insertStart)
		timings = append(timings, map[string]interface{}{"phase": "insert", "ms": insertDur.Milliseconds()})
		log.Printf("[UploadCashFlowCategorySimple] insert elapsed=%v file=%s", insertDur, fh.Filename)

		updateSQL := `
UPDATE mastercashflowcategory m
SET
	category_type = t.category_type,
	parent_category_name = t.parent_category_name,
	description = t.description,
	status = t.status,
	is_deleted = t.is_deleted
FROM tmp_mcc t
WHERE m.category_name = t.category_name
AND (
	m.category_type IS DISTINCT FROM t.category_type OR
	m.parent_category_name IS DISTINCT FROM t.parent_category_name OR
	m.description IS DISTINCT FROM t.description OR
	m.status IS DISTINCT FROM t.status OR
	m.is_deleted IS DISTINCT FROM t.is_deleted
);
`
		updateStart := time.Now()
		if _, err := tx.Exec(ctx, updateSQL); err != nil {
			api.RespondWithError(w, 500, constants.ErrUpdateFailed+err.Error())
			return
		}
		updateDur := time.Since(updateStart)
		timings = append(timings, map[string]interface{}{"phase": "update", "ms": updateDur.Milliseconds()})
		log.Printf("[UploadCashFlowCategorySimple] update elapsed=%v file=%s", updateDur, fh.Filename)

		hierarchySQL := `
WITH RECURSIVE affected AS (
	SELECT m.category_id, m.category_name, m.parent_category_name, 0 AS lvl
	FROM mastercashflowcategory m
	WHERE m.category_name IN (SELECT category_name FROM tmp_mcc)
	UNION ALL
	SELECT c.category_id, c.category_name, c.parent_category_name,
	       CASE WHEN a.lvl + 1 > 3 THEN 3 ELSE a.lvl + 1 END
	FROM mastercashflowcategory c
	JOIN affected a ON c.parent_category_name = a.category_name
	WHERE a.lvl < 3
)
UPDATE mastercashflowcategory m
SET category_level = a.lvl,
	is_top_level_category = (a.lvl = 0)
FROM affected a
WHERE m.category_id = a.category_id;
`
		hierarchyStart := time.Now()
		if _, err := tx.Exec(ctx, hierarchySQL); err != nil {
			api.RespondWithError(w, 500, "Hierarchy failed: "+err.Error())
			return
		}
		hierarchyDur := time.Since(hierarchyStart)
		timings = append(timings, map[string]interface{}{"phase": "hierarchy", "ms": hierarchyDur.Milliseconds()})
		log.Printf("[UploadCashFlowCategorySimple] hierarchy elapsed=%v file=%s", hierarchyDur, fh.Filename)

		relationshipSQL := `
INSERT INTO cashflowcategoryrelationships (parent_category_name, child_category_name, status)
SELECT DISTINCT p.category_name, c.category_name, 'Active'
FROM mastercashflowcategory c
JOIN mastercashflowcategory p ON c.parent_category_name = p.category_name
WHERE (p.category_name IN (SELECT category_name FROM tmp_mcc)
	OR c.category_name IN (SELECT category_name FROM tmp_mcc))
ON CONFLICT (parent_category_name, child_category_name) DO NOTHING;
`
		relStart := time.Now()
		if _, err := tx.Exec(ctx, relationshipSQL); err != nil {
			api.RespondWithError(w, 500, "Relationships failed: "+err.Error())
			return
		}
		relDur := time.Since(relStart)
		timings = append(timings, map[string]interface{}{"phase": "relationships", "ms": relDur.Milliseconds()})
		log.Printf("[UploadCashFlowCategorySimple] relationships elapsed=%v file=%s", relDur, fh.Filename)

		auditSQL := `
INSERT INTO auditactioncashflowcategory(category_id, actiontype, processing_status, requested_by, requested_at)
SELECT m.category_id, 'CREATE', 'PENDING_APPROVAL', $1, now()
FROM mastercashflowcategory m
WHERE m.category_name IN (SELECT category_name FROM tmp_mcc)
ON CONFLICT DO NOTHING;
`
		auditStart := time.Now()
		if _, err := tx.Exec(ctx, auditSQL, userName); err != nil {
			api.RespondWithError(w, 500, constants.ErrAuditInsertFailed+err.Error())
			return
		}
		auditDur := time.Since(auditStart)
		timings = append(timings, map[string]interface{}{"phase": "audit", "ms": auditDur.Milliseconds()})
		log.Printf("[UploadCashFlowCategorySimple] audit elapsed=%v file=%s", auditDur, fh.Filename)

		commitStart := time.Now()
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}
		commitDur := time.Since(commitStart)
		timings = append(timings, map[string]interface{}{"phase": "commit", "ms": commitDur.Milliseconds()})
		log.Printf("[UploadCashFlowCategorySimple] commit elapsed=%v file=%s", commitDur, fh.Filename)
		tx = nil

		dur := time.Since(startOverall)
		timings = append(timings, map[string]interface{}{"phase": "total", "ms": dur.Milliseconds()})
		resp := map[string]interface{}{
			constants.ValueSuccess: true,
			"file":                 fh.Filename,
			"rows":                 rowCount,
			"duration_ms":          dur.Milliseconds(),
			"batch_id":             uuid.New().String(),
			"timings":              timings,
		}
		log.Printf("[UploadCashFlowCategorySimple] finished rows=%d total_ms=%d file=%s", rowCount, dur.Milliseconds(), fh.Filename)
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}
