package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"

	// "log"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlyFolioError converts database errors into user-friendly messages
// Returns (user-friendly message, HTTP status code)
func getUserFriendlyFolioError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	errMsg := err.Error()

	// Unique constraint violations (HTTP 200 - user errors)
	if strings.Contains(errMsg, "unique_active_folio_per_entity_amc") ||
		strings.Contains(errMsg, "entity_name") && strings.Contains(errMsg, "amc_name") && strings.Contains(errMsg, "folio_number") {
		return "Folio number already exists for this entity and AMC combination. Please use a different folio number.", http.StatusOK
	}
	if strings.Contains(errMsg, constants.ErrDuplicateKey) {
		return "This folio already exists in the system.", http.StatusOK
	}

	// Foreign key violations (HTTP 200 - user errors)
	if strings.Contains(errMsg, "foreign key") || strings.Contains(errMsg, "fkey") {
		if strings.Contains(errMsg, "auditactionfolio") {
			return "Cannot perform this operation. Folio reference is invalid or has been deleted.", http.StatusOK
		}
		if strings.Contains(errMsg, "folioschememapping_folio_id_fkey") {
			return "Cannot perform this operation. Folio is referenced in scheme mappings.", http.StatusOK
		}
		if strings.Contains(errMsg, "folioschememapping_scheme_id_fkey") {
			return "Cannot perform this operation. Scheme reference is invalid.", http.StatusOK
		}
		if strings.Contains(errMsg, "transaction") || strings.Contains(errMsg, "subscription") || strings.Contains(errMsg, "redemption") {
			return "Cannot perform this operation. Folio has existing transactions.", http.StatusOK
		}
		return "Invalid reference. The related record does not exist.", http.StatusOK
	}

	// Check constraint violations (HTTP 200 - user errors)
	if strings.Contains(errMsg, "check constraint") {
		if strings.Contains(errMsg, "status_check") || strings.Contains(errMsg, "masterfolio_status_ck") {
			return "Invalid status. Must be 'Active', 'Inactive', or 'Closed'.", http.StatusOK
		}
		if strings.Contains(errMsg, "source_check") || strings.Contains(errMsg, "masterfolio_source_ck") {
			return "Invalid source. Must be 'Manual', 'Upload', or 'ERP'.", http.StatusOK
		}
		if strings.Contains(errMsg, "actiontype_check") || strings.Contains(errMsg, "auditactionfolio_actiontype_ck") {
			return "Invalid action type. Must be CREATE, EDIT, or DELETE.", http.StatusOK
		}
		if strings.Contains(errMsg, "processing_status_check") || strings.Contains(errMsg, "auditactionfolio_processing_status_ck") {
			return "Invalid processing status.", http.StatusOK
		}
		return "Invalid data provided. Please check your input.", http.StatusOK
	}

	// Not null violations (HTTP 200 - user errors)
	if strings.Contains(errMsg, "null value") || strings.Contains(errMsg, "violates not-null") {
		if strings.Contains(errMsg, "entity_name") {
			return "Entity name is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "amc_name") {
			return "AMC name is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "folio_number") {
			return "Folio number is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "first_holder_name") {
			return "First holder name is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "default_subscription_account") {
			return "Default subscription account is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "default_redemption_account") {
			return "Default redemption account is required.", http.StatusOK
		}
		return "Required field is missing.", http.StatusOK
	}

	// Connection errors (HTTP 503 Service Unavailable)
	if strings.Contains(errMsg, "connection") || strings.Contains(errMsg, "timeout") {
		return "Database connection error. Please try again.", http.StatusServiceUnavailable
	}

	// Return original error with context (HTTP 500)
	if context != "" {
		return context + ": " + errMsg, http.StatusInternalServerError
	}
	return errMsg, http.StatusInternalServerError
}

func UploadFolio(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startOverall := time.Now()
		ctx := r.Context()
		timings := []map[string]interface{}{}

		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			var tmp struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&tmp)
			userID = tmp.UserID
		}
		if userID == "" {
			api.RespondWithError(w, 400, constants.ErrUserIDRequired)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, 401, constants.ErrInvalidSession)
			return
		}

		if err := r.ParseMultipartForm(64 << 20); err != nil {
			msg, status := getUserFriendlyFolioError(err, "form parse")
			api.RespondWithError(w, status, msg)
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, 400, constants.ErrNoFileUploaded)
			return
		}

		results := []map[string]interface{}{}

		for _, fh := range files {
			fileStart := time.Now()
			f, err := fh.Open()
			if err != nil {
				msg, status := getUserFriendlyFolioError(err, "open file")
				api.RespondWithError(w, status, msg)
				return
			}
			defer f.Close()

			records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, 400, "invalid csv")
				return
			}

			readDur := time.Since(fileStart)
			timings = append(timings, map[string]interface{}{"phase": "read_csv", "rows": len(records) - 1, "ms": readDur.Milliseconds()})

			headers := normalizeHeader(records[0])
			dataRows := records[1:]
			headerPos := map[string]int{}
			for i, h := range headers {
				headerPos[h] = i
			}

			required := []string{"entity_name", "amc_name", "folio_number", "first_holder_name", "default_subscription_account", "default_redemption_account"}
			for _, c := range required {
				if _, ok := headerPos[c]; !ok {
					api.RespondWithError(w, 400, fmt.Sprintf("missing column: %s", c))
					return
				}
			}

			// Get context-based approved data for validations
			approvedEntities, _ := ctx.Value(api.BusinessUnitsKey).([]string)
			approvedAMCs, _ := ctx.Value("ApprovedAMCs").([]map[string]string)
			approvedBankAccounts, _ := ctx.Value("ApprovedBankAccounts").([]map[string]string)
			approvedSchemes, _ := ctx.Value("ApprovedSchemes").([]map[string]string)

			// Validate entities, AMCs, and bank accounts from uploaded data
			for _, r := range dataRows {
				// Validate entity
				entityName := strings.TrimSpace(r[headerPos["entity_name"]])
				entityFound := false
				for _, e := range approvedEntities {
					if strings.EqualFold(e, entityName) {
						entityFound = true
						break
					}
				}
				if !entityFound {
					api.RespondWithError(w, 403, "Access denied. Entity '"+entityName+"' not in your accessible entities")
					return
				}

				// Validate AMC
				amcName := strings.TrimSpace(r[headerPos["amc_name"]])
				amcFound := false
				for _, amc := range approvedAMCs {
					if strings.EqualFold(amc["amc_name"], amcName) {
						amcFound = true
						break
					}
				}
				if !amcFound {
					api.RespondWithError(w, 400, constants.ErrAMCNotFoundOrNotApprovedActive+amcName)
					return
				}

				// Validate default_subscription_account
				subAcct := strings.TrimSpace(r[headerPos["default_subscription_account"]])
				subAcctFound := false
				for _, acc := range approvedBankAccounts {
					if acc["account_number"] == subAcct || acc["account_id"] == subAcct {
						subAcctFound = true
						break
					}
				}
				if !subAcctFound {
					api.RespondWithError(w, 400, "Subscription account not found or not approved/active: "+subAcct)
					return
				}

				// Validate default_redemption_account
				redAcct := strings.TrimSpace(r[headerPos["default_redemption_account"]])
				redAcctFound := false
				for _, acc := range approvedBankAccounts {
					if acc["account_number"] == redAcct || acc["account_id"] == redAcct {
						redAcctFound = true
						break
					}
				}
				if !redAcctFound {
					api.RespondWithError(w, 400, "Redemption account not found or not approved/active: "+redAcct)
					return
				}
			}

			schemeRefs := map[string]struct{}{}
			hasSchemeCol := false
			if pos, ok := headerPos["scheme_ids"]; ok {
				hasSchemeCol = true
				for _, r := range dataRows {
					if pos < len(r) {
						for _, ref := range strings.Split(r[pos], ",") {
							ref = strings.TrimSpace(ref)
							if ref != "" {
								schemeRefs[ref] = struct{}{}
							}
						}
					}
				}
			}

			// Validate schemes from context instead of database query
			schemeMap := map[string]string{}
			if len(schemeRefs) > 0 {
				for ref := range schemeRefs {
					refTrimmed := strings.TrimSpace(ref)
					for _, scheme := range approvedSchemes {
						if scheme["scheme_id"] == refTrimmed ||
							strings.EqualFold(scheme["scheme_name"], refTrimmed) ||
							scheme["isin"] == refTrimmed ||
							scheme["internal_scheme_code"] == refTrimmed {
							schemeMap[refTrimmed] = scheme["scheme_id"]
							break
						}
					}
				}
			}

			copyCols := []string{
				"entity_name", "amc_name", "folio_number", "first_holder_name",
				"default_subscription_account", "default_redemption_account", constants.KeyStatus, "source",
			}
			copyRows := make([][]interface{}, 0, len(dataRows))
			for _, r := range dataRows {
				row := make([]interface{}, len(copyCols))
				for j, c := range copyCols {
					switch c {
					case constants.KeyStatus:
						row[j] = "Active"
					case "source":
						row[j] = "Upload"
					default:
						if pos, ok := headerPos[c]; ok && pos < len(r) {
							row[j] = strings.TrimSpace(r[pos])
						} else {
							row[j] = nil
						}
					}
				}
				copyRows = append(copyRows, row)
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				msg, status := getUserFriendlyFolioError(err, "tx")
				api.RespondWithError(w, status, msg)
				return
			}
			defer tx.Rollback(ctx)
			_, _ = tx.Exec(ctx, "SET LOCAL synchronous_commit TO OFF")

			_, err = tx.Exec(ctx, `
				DROP TABLE IF EXISTS tmp_folio;
				CREATE TEMP TABLE tmp_folio (LIKE investment.masterfolio INCLUDING DEFAULTS) ON COMMIT DROP;
			`)
			if err != nil {
				msg, status := getUserFriendlyFolioError(err, "tmp table")
				api.RespondWithError(w, status, msg)
				return
			}

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_folio"}, copyCols, pgx.CopyFromRows(copyRows)); err != nil {
				msg, status := getUserFriendlyFolioError(err, "copy")
				api.RespondWithError(w, status, msg)
				return
			}

			updateSQL := `
	UPDATE investment.masterfolio m
	SET 
		first_holder_name = COALESCE(t.first_holder_name, m.first_holder_name),
		default_subscription_account = COALESCE(t.default_subscription_account, m.default_subscription_account),
		default_redemption_account = COALESCE(t.default_redemption_account, m.default_redemption_account),
		status = COALESCE(t.status, m.status),
		source = COALESCE(t.source, m.source)
	FROM tmp_folio t
	WHERE m.entity_name = t.entity_name
	  AND m.amc_name = t.amc_name
	  AND m.folio_number = t.folio_number
	  AND m.is_deleted = false;
`
			if _, err := tx.Exec(ctx, updateSQL); err != nil {
				msg, status := getUserFriendlyFolioError(err, "update masterfolio")
				api.RespondWithError(w, status, msg)
				return
			}

			insertSQL := `
	INSERT INTO investment.masterfolio (
		entity_name, amc_name, folio_number, first_holder_name,
		default_subscription_account, default_redemption_account,
		status, source
	)
	SELECT t.entity_name, t.amc_name, t.folio_number, t.first_holder_name,
		   t.default_subscription_account, t.default_redemption_account,
		   COALESCE(t.status,'Active'), COALESCE(t.source,'Upload')
	FROM tmp_folio t
	WHERE NOT EXISTS (
		SELECT 1 FROM investment.masterfolio m
		WHERE m.entity_name = t.entity_name
		  AND m.amc_name = t.amc_name
		  AND m.folio_number = t.folio_number
		  AND m.is_deleted = false
	);
`
			if _, err := tx.Exec(ctx, insertSQL); err != nil {
				msg, status := getUserFriendlyFolioError(err, "insert masterfolio")
				api.RespondWithError(w, status, msg)
				return
			}

			auditSQL := `
				INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, requested_by, requested_at)
				SELECT m.folio_id, 'CREATE', 'PENDING_APPROVAL', $1, now()
				FROM investment.masterfolio m
				JOIN tmp_folio t USING (folio_number)
				ON CONFLICT DO NOTHING;
			`
			if _, err := tx.Exec(ctx, auditSQL, userEmail); err != nil {
				msg, status := getUserFriendlyFolioError(err, "audit insert")
				api.RespondWithError(w, status, msg)
				return
			}

			if hasSchemeCol && len(schemeMap) > 0 {
				_, err = tx.Exec(ctx, `
					DROP TABLE IF EXISTS tmp_folio_scheme;
					CREATE TEMP TABLE tmp_folio_scheme (
						folio_number varchar(50),
						scheme_id varchar(50),
						status varchar(20)
					) ON COMMIT DROP;
				`)
				if err != nil {
					msg, status := getUserFriendlyFolioError(err, "mapping tmp")
					api.RespondWithError(w, status, msg)
					return
				}

				pos := headerPos["scheme_ids"]
				mappingRows := [][]interface{}{}
				for _, r := range dataRows {
					folioNum := strings.TrimSpace(r[headerPos["folio_number"]])
					if pos < len(r) {
						for _, ref := range strings.Split(r[pos], ",") {
							ref = strings.TrimSpace(ref)
							if sid, ok := schemeMap[ref]; ok && sid != "" {
								mappingRows = append(mappingRows, []interface{}{folioNum, sid, "Active"})
							}
						}
					}
				}

				if len(mappingRows) > 0 {
					if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_folio_scheme"},
						[]string{"folio_number", "scheme_id", constants.KeyStatus},
						pgx.CopyFromRows(mappingRows)); err != nil {
						msg, status := getUserFriendlyFolioError(err, "mapping copy")
						api.RespondWithError(w, status, msg)
						return
					}

					if _, err := tx.Exec(ctx, `
						INSERT INTO investment.folioschememapping (folio_id, scheme_id, status)
						SELECT m.folio_id, t.scheme_id, t.status
						FROM tmp_folio_scheme t
						JOIN investment.masterfolio m ON m.folio_number = t.folio_number
						ON CONFLICT (folio_id, scheme_id) DO NOTHING;
					`); err != nil {
						msg, status := getUserFriendlyFolioError(err, "mapping insert")
						api.RespondWithError(w, status, msg)
						return
					}
				}
			}
			if err := tx.Commit(ctx); err != nil {
				msg, status := getUserFriendlyFolioError(err, "commit")
				api.RespondWithError(w, status, msg)
				return
			}

			results = append(results, map[string]interface{}{
				"batch_id":             uuid.New().String(),
				"rows":                 len(copyRows),
				constants.ValueSuccess: true,
				"elapsed":              time.Since(fileStart).Milliseconds(),
			})
		}

		total := time.Since(startOverall).Milliseconds()
		api.RespondWithPayload(w, true, "", map[string]any{
			constants.ValueSuccess: true,
			"processed":            len(results),
			"batches":              results,
			"total_ms":             total,
			"rows_count":           len(results),
		})
	}
}

func GetFoliosWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Get user's accessible entities from context
		approvedEntities, _ := ctx.Value(api.BusinessUnitsKey).([]string)
		if len(approvedEntities) == 0 {
			api.RespondWithError(w, http.StatusForbidden, "No accessible entities found")
			return
		}

		// Get approved AMCs from context
		approvedAMCs, _ := ctx.Value("ApprovedAMCs").([]map[string]string)
		amcNames := make([]string, 0, len(approvedAMCs))
		for _, amc := range approvedAMCs {
			if amc["amc_name"] != "" {
				amcNames = append(amcNames, amc["amc_name"])
			}
		}

		// Get approved bank accounts from context
		approvedBankAccounts, _ := ctx.Value("ApprovedBankAccounts").([]map[string]string)
		accountIdentifiers := make([]string, 0, len(approvedBankAccounts)*2)
		for _, acc := range approvedBankAccounts {
			// Include both account_id and account_number for filtering
			if acc["account_id"] != "" {
				accountIdentifiers = append(accountIdentifiers, acc["account_id"])
			}
			if acc["account_number"] != "" {
				accountIdentifiers = append(accountIdentifiers, acc["account_number"])
			}
		}

		// Get approved schemes from context
		approvedSchemes, _ := ctx.Value("ApprovedSchemes").([]map[string]string)
		schemeIDs := make([]string, 0, len(approvedSchemes))
		for _, scheme := range approvedSchemes {
			if scheme["scheme_id"] != "" {
				schemeIDs = append(schemeIDs, scheme["scheme_id"])
			}
		}

		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.folio_id)
					a.folio_id,
					a.actiontype,
					a.processing_status,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactionfolio a
				ORDER BY a.folio_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					folio_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactionfolio
				GROUP BY folio_id
			),
			schemes AS (
				SELECT 
					fsm.folio_id,
					json_agg(
						json_build_object(
							'scheme_id', s.scheme_id,
							'scheme_name', s.scheme_name,
							'isin', s.isin,
							'internal_scheme_code', s.internal_scheme_code,
							'mapping_status', fsm.status
						)
					) AS scheme_list
				FROM investment.folioschememapping fsm
				JOIN investment.masterscheme s ON s.scheme_id = fsm.scheme_id
				WHERE s.scheme_id = ANY($4)
				GROUP BY fsm.folio_id
			),
			clearing AS (
				SELECT 
					c.account_id,
					STRING_AGG(c.code_type || ':' || c.code_value, ', ') AS clearing_codes
				FROM public.masterclearingcode c
				GROUP BY c.account_id
			),
			sub_ac AS (
				SELECT 
					ba.account_number,
					ba.account_nickname,
					ba.account_id,
					b.bank_name,
					COALESCE(e.entity_name, ec.entity_name) AS entity_name,
					cl.clearing_codes
				FROM public.masterbankaccount ba
				LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
				LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
				LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
				LEFT JOIN clearing cl ON cl.account_id = ba.account_id
			),
			red_ac AS (
				SELECT 
					ba.account_number,
					ba.account_nickname,
					ba.account_id,
					b.bank_name,
					COALESCE(e.entity_name, ec.entity_name) AS entity_name,
					cl.clearing_codes
				FROM public.masterbankaccount ba
				LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
				LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
				LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
				LEFT JOIN clearing cl ON cl.account_id = ba.account_id
			)
			SELECT
				m.folio_id,
				m.entity_name,
				m.old_entity_name,
				m.amc_name,
				m.old_amc_name,
				m.folio_number,
				m.old_folio_number,
				m.first_holder_name,
				m.old_first_holder_name,
				m.default_subscription_account,
				m.old_default_subscription_account,
				m.default_redemption_account,
				m.old_default_redemption_account,
				m.status,
				m.old_status,
				m.source,
				m.old_source,
				m.is_deleted,

				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,

				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				COALESCE(h.deleted_by,'') AS deleted_by,
				COALESCE(h.deleted_at,'') AS deleted_at,

				sub.account_number AS sub_account_number,
				sub.account_nickname AS sub_account_nickname,
				sub.bank_name AS sub_bank_name,
				sub.entity_name AS sub_entity_name,
				sub.clearing_codes AS sub_clearing_codes,

				red.account_number AS red_account_number,
				red.account_nickname AS red_account_nickname,
				red.bank_name AS red_bank_name,
				red.entity_name AS red_entity_name,
				red.clearing_codes AS red_clearing_codes,

				COALESCE(sch.scheme_list, '[]') AS schemes

		FROM investment.masterfolio m
		LEFT JOIN latest_audit l ON l.folio_id = m.folio_id
		LEFT JOIN history h ON h.folio_id = m.folio_id
		LEFT JOIN sub_ac sub ON (sub.account_number = m.default_subscription_account OR sub.account_id = m.default_subscription_account)
		LEFT JOIN red_ac red ON (red.account_number = m.default_redemption_account OR red.account_id = m.default_redemption_account)
		LEFT JOIN schemes sch ON sch.folio_id = m.folio_id
		WHERE COALESCE(m.is_deleted,false)=false
		  AND m.entity_name = ANY($1)
		  AND m.amc_name = ANY($2)
		  AND (m.default_subscription_account = ANY($3) OR m.default_redemption_account = ANY($3))
		ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC	;
	`

		rows, err := pgxPool.Query(ctx, q, approvedEntities, amcNames, accountIdentifiers, schemeIDs)
		if err != nil {
			msg, status := getUserFriendlyFolioError(err, "query")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)

		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				name := string(f.Name)
				if vals[i] == nil {
					rec[name] = ""
				} else if t, ok := vals[i].(time.Time); ok {
					rec[name] = t.Format(constants.DateTimeFormat)
				} else {
					rec[name] = vals[i]
				}
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			msg, status := getUserFriendlyFolioError(rows.Err(), "scan")
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func GetApprovedActiveFolios(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Get user's accessible entities from context
		approvedEntities, _ := ctx.Value(api.BusinessUnitsKey).([]string)
		if len(approvedEntities) == 0 {
			api.RespondWithError(w, http.StatusForbidden, "No accessible entities found")
			return
		}

		// Get approved AMCs from context
		approvedAMCs, _ := ctx.Value("ApprovedAMCs").([]map[string]string)
		amcNames := make([]string, 0, len(approvedAMCs))
		for _, amc := range approvedAMCs {
			if amc["amc_name"] != "" {
				amcNames = append(amcNames, amc["amc_name"])
			}
		}

		// Get approved bank accounts from context
		approvedBankAccounts, _ := ctx.Value("ApprovedBankAccounts").([]map[string]string)
		accountIdentifiers := make([]string, 0, len(approvedBankAccounts))
		for _, acc := range approvedBankAccounts {
			if acc["account_number"] != "" {
				accountIdentifiers = append(accountIdentifiers, acc["account_number"])
			}
		}

		q := `
			WITH latest AS (
				SELECT DISTINCT ON (folio_id)
					folio_id,
					processing_status
				FROM investment.auditactionfolio
				ORDER BY folio_id, requested_at DESC
			)
			SELECT
				m.folio_id,
				m.entity_name,
				m.amc_name,
				m.folio_number,
				m.first_holder_name,
				m.default_subscription_account,
				m.default_redemption_account
			FROM investment.masterfolio m
			JOIN latest l ON l.folio_id = m.folio_id
			WHERE 
				UPPER(l.processing_status) = 'APPROVED'
				AND UPPER(m.status) = 'ACTIVE'
				AND COALESCE(m.is_deleted,false)=false
				AND m.entity_name = ANY($1)
				AND m.amc_name = ANY($2)
				AND (m.default_subscription_account = ANY($3) OR m.default_redemption_account = ANY($3))
			ORDER BY m.entity_name, m.folio_number;
		`

		rows, err := pgxPool.Query(ctx, q, approvedEntities, amcNames, accountIdentifiers)
		if err != nil {
			msg, status := getUserFriendlyFolioError(err, "query")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{})
			for i, fd := range rows.FieldDescriptions() {
				if vals[i] == nil {
					rec[string(fd.Name)] = ""
				} else {
					rec[string(fd.Name)] = vals[i]
				}
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			msg, status := getUserFriendlyFolioError(rows.Err(), "scan")
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func CreateFolioSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                  string   `json:"user_id"`
			EntityName              string   `json:"entity_name"`
			AMCName                 string   `json:"amc_name"`
			FolioNumber             string   `json:"folio_number"`
			FirstHolderName         string   `json:"first_holder_name"`
			DefaultSubscriptionAcct string   `json:"default_subscription_account"`
			DefaultRedemptionAcct   string   `json:"default_redemption_account"`
			Status                  string   `json:"status,omitempty"`
			SchemeRefs              []string `json:"scheme_ids,omitempty"` // may be ids/names/isin etc.
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		// required
		if strings.TrimSpace(req.EntityName) == "" || strings.TrimSpace(req.AMCName) == "" ||
			strings.TrimSpace(req.FolioNumber) == "" || strings.TrimSpace(req.FirstHolderName) == "" ||
			strings.TrimSpace(req.DefaultSubscriptionAcct) == "" || strings.TrimSpace(req.DefaultRedemptionAcct) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrMissingRequiredFieldsUser)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Validate entity access
		approvedEntities, _ := ctx.Value(api.BusinessUnitsKey).([]string)
		if len(approvedEntities) == 0 {
			api.RespondWithError(w, http.StatusForbidden, "No accessible entities found in context")
			return
		}
		entityFound := false
		for _, e := range approvedEntities {
			if strings.EqualFold(e, req.EntityName) {
				entityFound = true
				break
			}
		}
		if !entityFound {
			api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Access denied. Entity '%s' not in your accessible entities: %v", req.EntityName, approvedEntities))
			return
		}

		// Validate AMC
		approvedAMCs, _ := ctx.Value("ApprovedAMCs").([]map[string]string)
		amcFound := false
		for _, amc := range approvedAMCs {
			if strings.EqualFold(amc["amc_name"], req.AMCName) {
				amcFound = true
				break
			}
		}
		if !amcFound {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrAMCNotFoundOrNotApprovedActive+req.AMCName)
			return
		}

		// Validate bank accounts
		approvedBankAccounts, _ := ctx.Value("ApprovedBankAccounts").([]map[string]string)
		if len(approvedBankAccounts) == 0 {
			api.RespondWithError(w, http.StatusForbidden, "No approved bank accounts found in context")
			return
		}
		subAcctFound := false
		for _, acc := range approvedBankAccounts {
			if acc["account_number"] == req.DefaultSubscriptionAcct || acc["account_id"] == req.DefaultSubscriptionAcct {
				subAcctFound = true
				break
			}
		}
		if !subAcctFound {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Subscription account '%s' not found or not approved/active. Available accounts: %d", req.DefaultSubscriptionAcct, len(approvedBankAccounts)))
			return
		}

		redAcctFound := false
		for _, acc := range approvedBankAccounts {
			if acc["account_number"] == req.DefaultRedemptionAcct || acc["account_id"] == req.DefaultRedemptionAcct {
				redAcctFound = true
				break
			}
		}
		if !redAcctFound {
			api.RespondWithError(w, http.StatusBadRequest, "Redemption account not found or not approved/active: "+req.DefaultRedemptionAcct)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFolioError(err, "tx")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// uniqueness check
		var existing string
		err = tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE entity_name=$1 AND amc_name=$2 AND folio_number=$3 AND COALESCE(is_deleted,false)=false LIMIT 1`,
			req.EntityName, req.AMCName, req.FolioNumber).Scan(&existing)
		if err == nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFolioAlreadyExistsUser)
			return
		}

		var folioID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.masterfolio
				(entity_name, amc_name, folio_number, first_holder_name, default_subscription_account, default_redemption_account, status, source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,'Manual')
				RETURNING folio_id
			`, req.EntityName, req.AMCName, req.FolioNumber, req.FirstHolderName, req.DefaultSubscriptionAcct, req.DefaultRedemptionAcct, defaultIfEmpty(req.Status, "Active")).Scan(&folioID); err != nil {
			msg, status := getUserFriendlyFolioError(err, "insert")
			api.RespondWithError(w, status, msg)
			return
		}

		// resolve scheme refs using context instead of database query
		if len(req.SchemeRefs) > 0 {
			approvedSchemes, _ := ctx.Value("ApprovedSchemes").([]map[string]string)
			mappingRows := [][]interface{}{}

			for _, ref := range req.SchemeRefs {
				refTrimmed := strings.TrimSpace(ref)
				if refTrimmed == "" {
					continue
				}

				// Find scheme in approved schemes from context
				for _, scheme := range approvedSchemes {
					if scheme["scheme_id"] == refTrimmed ||
						strings.EqualFold(scheme["scheme_name"], refTrimmed) ||
						scheme["isin"] == refTrimmed ||
						scheme["internal_scheme_code"] == refTrimmed {
						mappingRows = append(mappingRows, []interface{}{folioID, scheme["scheme_id"], "Active", nil})
						break
					}
				}
			}

			if len(mappingRows) > 0 {
				if _, err := tx.CopyFrom(ctx, pgx.Identifier{"investment", "folioschememapping"},
					[]string{"folio_id", "scheme_id", constants.KeyStatus, "old_status"}, pgx.CopyFromRows(mappingRows)); err != nil {
					msg, status := getUserFriendlyFolioError(err, "mapping insert")
					api.RespondWithError(w, status, msg)
					return
				}
			}
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
		`, folioID, userEmail); err != nil {
			msg, status := getUserFriendlyFolioError(err, "audit")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyFolioError(err, "commit")
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"folio_id": folioID, "requested": userEmail})
	}
}

func CreateFolioBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				EntityName              string   `json:"entity_name"`
				AMCName                 string   `json:"amc_name"`
				FolioNumber             string   `json:"folio_number"`
				FirstHolderName         string   `json:"first_holder_name"`
				DefaultSubscriptionAcct string   `json:"default_subscription_account"`
				DefaultRedemptionAcct   string   `json:"default_redemption_account"`
				Status                  string   `json:"status,omitempty"`
				SchemeRefs              []string `json:"scheme_ids,omitempty"`
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

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxBeginFailedCapitalized + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			// validations
			if strings.TrimSpace(row.EntityName) == "" || strings.TrimSpace(row.AMCName) == "" ||
				strings.TrimSpace(row.FolioNumber) == "" || strings.TrimSpace(row.FirstHolderName) == "" ||
				strings.TrimSpace(row.DefaultSubscriptionAcct) == "" || strings.TrimSpace(row.DefaultRedemptionAcct) == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "missing required fields"})
				continue
			}

			// uniqueness
			var exists string
			err = tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE entity_name=$1 AND amc_name=$2 AND folio_number=$3 AND COALESCE(is_deleted,false)=false LIMIT 1`,
				row.EntityName, row.AMCName, row.FolioNumber).Scan(&exists)
			if err == nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "entity": row.EntityName, "folio_number": row.FolioNumber, constants.ValueError: "duplicate"})
				continue
			}

			var folioID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.masterfolio
					(entity_name, amc_name, folio_number, first_holder_name, default_subscription_account, default_redemption_account, status, source)
				VALUES ($1,$2,$3,$4,$5,$6,$7,'Manual')
				RETURNING folio_id
			`, row.EntityName, row.AMCName, row.FolioNumber, row.FirstHolderName, row.DefaultSubscriptionAcct, row.DefaultRedemptionAcct, defaultIfEmpty(row.Status, "Active")).Scan(&folioID); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "insert failed: " + err.Error()})
				continue
			}

			// mapping
			if len(row.SchemeRefs) > 0 {
				refList := []string{}
				for _, s := range row.SchemeRefs {
					if ss := strings.TrimSpace(s); ss != "" {
						refList = append(refList, ss)
					}
				}
				if len(refList) > 0 {
					rowsS, err := tx.Query(ctx, `
						SELECT scheme_id, scheme_name, isin, internal_scheme_code
						FROM investment.masterscheme
						WHERE scheme_id = ANY($1) OR scheme_name = ANY($1) OR isin = ANY($1) OR internal_scheme_code = ANY($1)
					`, refList)
					if err == nil {
						mappingRows := [][]interface{}{}
						for rowsS.Next() {
							var sid, sname, isin, icode string
							_ = rowsS.Scan(&sid, &sname, &isin, &icode)
							mappingRows = append(mappingRows, []interface{}{folioID, sid, "Active", nil})
						}
						if len(mappingRows) > 0 {
							if _, err := tx.CopyFrom(ctx, pgx.Identifier{"investment", "folioschememapping"},
								[]string{"folio_id", "scheme_id", constants.KeyStatus, "old_status"}, pgx.CopyFromRows(mappingRows)); err != nil {
								results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "mapping insert failed: " + err.Error()})
								continue
							}
						}
					}
				}
			}

			// audit
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, requested_by, requested_at)
				VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
			`, folioID, userEmail); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrAuditInsertFailed + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailed + err.Error()})
				continue
			}
			results = append(results, map[string]interface{}{constants.ValueSuccess: true, "folio_id": folioID})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

func UpdateFolio(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string                 `json:"user_id"`
			FolioID string                 `json:"folio_id"`
			Fields  map[string]interface{} `json:"fields"`
			Reason  string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.FolioID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFolioIDRequiredUser)
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFieldsToUpdateUser)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFolioError(err, "tx")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// select existing for old_* values
		var oldVals [7]interface{}
		sel := `
			SELECT entity_name, amc_name, folio_number, first_holder_name,
			       default_subscription_account, default_redemption_account, status
			FROM investment.masterfolio
			WHERE folio_id=$1 FOR UPDATE
		`
		if err := tx.QueryRow(ctx, sel, req.FolioID).Scan(&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4], &oldVals[5], &oldVals[6]); err != nil {
			msg, status := getUserFriendlyFolioError(err, "fetch")
			api.RespondWithError(w, status, msg)
			return
		}

		fieldPairs := map[string]int{
			"entity_name":                  0,
			"amc_name":                     1,
			"folio_number":                 2,
			"first_holder_name":            3,
			"default_subscription_account": 4,
			"default_redemption_account":   5,
			constants.KeyStatus:            6,
		}

		var sets []string
		var args []interface{}
		pos := 1
		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
				// if folio_number change -> uniqueness check
				if lk == "folio_number" {
					newVal := strings.TrimSpace(fmt.Sprint(v))
					if newVal != "" && newVal != ifaceToString(oldVals[2]) {
						var exists string
						err := tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number=$1 AND entity_name=$2 AND amc_name=$3 AND COALESCE(is_deleted,false)=false LIMIT 1`,
							newVal, ifaceToString(oldVals[0]), ifaceToString(oldVals[1])).Scan(&exists)
						if err == nil {
							api.RespondWithError(w, http.StatusBadRequest, "folio_number already exists for this entity+amc")
							return
						}
					}
				}
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

		q := fmt.Sprintf("UPDATE investment.masterfolio SET %s WHERE folio_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.FolioID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			msg, status := getUserFriendlyFolioError(err, "update")
			api.RespondWithError(w, status, msg)
			return
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
		`, req.FolioID, req.Reason, userEmail); err != nil {
			msg, status := getUserFriendlyFolioError(err, "audit")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyFolioError(err, "commit")
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"folio_id": req.FolioID, "requested": userEmail})
	}
}

func UpdateFolioBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				FolioID string                 `json:"folio_id"`
				Fields  map[string]interface{} `json:"fields"`
				Reason  string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if row.FolioID == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrFolioIDMissingUser})
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "folio_id": row.FolioID, constants.ValueError: constants.ErrTxBeginFailedCapitalized + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			var oldVals [7]interface{}
			if err := tx.QueryRow(ctx, `
				SELECT entity_name, amc_name, folio_number, first_holder_name,
				       default_subscription_account, default_redemption_account, status
				FROM investment.masterfolio WHERE folio_id=$1 FOR UPDATE
			`, row.FolioID).Scan(&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4], &oldVals[5], &oldVals[6]); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "folio_id": row.FolioID, constants.ValueError: "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"entity_name":                  0,
				"amc_name":                     1,
				"folio_number":                 2,
				"first_holder_name":            3,
				"default_subscription_account": 4,
				"default_redemption_account":   5,
				constants.KeyStatus:            6,
			}

			var sets []string
			var args []interface{}
			pos := 1
			for k, v := range row.Fields {
				lk := strings.ToLower(k)
				if idx, ok := fieldPairs[lk]; ok {
					// uniqueness check for folio_number
					if lk == "folio_number" {
						newVal := strings.TrimSpace(fmt.Sprint(v))
						if newVal != "" && newVal != ifaceToString(oldVals[2]) {
							var exists string
							err := tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number=$1 AND entity_name=$2 AND amc_name=$3 AND COALESCE(is_deleted,false)=false LIMIT 1`,
								newVal, ifaceToString(oldVals[0]), ifaceToString(oldVals[1])).Scan(&exists)
							if err == nil {
								results = append(results, map[string]interface{}{constants.ValueSuccess: false, "folio_id": row.FolioID, constants.ValueError: "folio_number already exists"})
								continue
							}
						}
					}
					oldField := "old_" + lk
					sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, lk, pos, oldField, pos+1))
					args = append(args, v, oldVals[idx])
					pos += 2
				}
			}
			if len(sets) == 0 {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "folio_id": row.FolioID, constants.ValueError: "No valid fields"})
				continue
			}
			q := fmt.Sprintf("UPDATE investment.masterfolio SET %s WHERE folio_id=$%d", strings.Join(sets, ", "), pos)
			args = append(args, row.FolioID)
			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "folio_id": row.FolioID, constants.ValueError: constants.ErrUpdateFailed + err.Error()})
				continue
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
			`, row.FolioID, row.Reason, userEmail); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "folio_id": row.FolioID, constants.ValueError: constants.ErrAuditInsertFailed + err.Error()})
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "folio_id": row.FolioID, constants.ValueError: constants.ErrCommitFailed + err.Error()})
				continue
			}
			results = append(results, map[string]interface{}{constants.ValueSuccess: true, "folio_id": row.FolioID})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

func DeleteFolio(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			FolioIDs []string `json:"folio_ids"`
			Reason   string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if len(req.FolioIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "folio_ids required")
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFolioError(err, "tx")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.FolioIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())
			`, id, req.Reason, requestedBy); err != nil {
				msg, status := getUserFriendlyFolioError(err, "audit")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyFolioError(err, "commit")
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.FolioIDs})
	}
}

func BulkApproveFolioActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			FolioIDs []string `json:"folio_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFolioError(err, "tx")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (folio_id) action_id, folio_id, actiontype, processing_status
			FROM investment.auditactionfolio
			WHERE folio_id = ANY($1)
			ORDER BY folio_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.FolioIDs)
		if err != nil {
			msg, status := getUserFriendlyFolioError(err, "query")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		var toApprove []string
		var toDeleteActionIDs []string
		var deleteMasterIDs []string

		for rows.Next() {
			var aid, fid, atype, pstatus string
			if err := rows.Scan(&aid, &fid, &atype, &pstatus); err != nil {
				continue
			}
			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			if ps == "APPROVED" {
				continue
			}
			if ps == "PENDING_DELETE_APPROVAL" {
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteMasterIDs = append(deleteMasterIDs, fid)
				continue
			}
			if ps == "PENDING_APPROVAL" || ps == "PENDING_EDIT_APPROVAL" {
				toApprove = append(toApprove, aid)
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrNoApprovableActions, map[string]any{
				"approved_action_ids": []string{},
				"deleted_folios":      []string{},
			})
			return
		}

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionfolio
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove); err != nil {
				msg, status := getUserFriendlyFolioError(err, "approve update")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionfolio
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				msg, status := getUserFriendlyFolioError(err, "mark deleted")
				api.RespondWithError(w, status, msg)
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE investment.masterfolio
				SET is_deleted=true, status='Inactive'
				WHERE folio_id = ANY($1)
			`, deleteMasterIDs); err != nil {
				msg, status := getUserFriendlyFolioError(err, "soft delete")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyFolioError(err, "commit")
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"approved_action_ids": toApprove,
			"deleted_folios":      deleteMasterIDs,
		})
	}
}

func BulkRejectFolioActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			FolioIDs []string `json:"folio_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyFolioError(err, "tx")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (folio_id) action_id, folio_id, processing_status
			FROM investment.auditactionfolio
			WHERE folio_id = ANY($1)
			ORDER BY folio_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.FolioIDs)
		if err != nil {
			msg, status := getUserFriendlyFolioError(err, "query")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		actionIDs := []string{}
		cannotReject := []string{}
		found := map[string]bool{}
		for rows.Next() {
			var aid, fid, ps string
			if err := rows.Scan(&aid, &fid, &ps); err != nil {
				continue
			}
			found[fid] = true
			if strings.ToUpper(strings.TrimSpace(ps)) == "APPROVED" {
				cannotReject = append(cannotReject, fid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		missing := []string{}
		for _, id := range req.FolioIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf(constants.ErrNoAuditActionFoundFormat, missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf(constants.ErrCannotRejectApprovedFormat, cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactionfolio
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE action_id = ANY($3)
		`, checkerBy, req.Comment, actionIDs); err != nil {
			msg, status := getUserFriendlyFolioError(err, "update")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyFolioError(err, "commit")
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

func GetSingleFolioWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqStruct struct {
		FolioID string `json:"folio_id"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var req reqStruct
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.FolioID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "folio_id is required in body")
			return
		}

		q := `
            WITH latest_audit AS (
                SELECT DISTINCT ON (a.folio_id)
                    a.folio_id, a.actiontype, a.processing_status, a.action_id,
                    a.requested_by, a.requested_at, a.checker_by, a.checker_at,
                    a.checker_comment, a.reason
                FROM investment.auditactionfolio a
                ORDER BY a.folio_id, a.requested_at DESC
            ),
            history AS (
                SELECT folio_id,
                    MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
                    MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
                    MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
                    MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
                    MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
                    MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
                FROM investment.auditactionfolio
                GROUP BY folio_id
            ),
            schemes AS (
                SELECT fsm.folio_id,
                    json_agg(
                        json_build_object(
                            'scheme_id', s.scheme_id,
                            'scheme_name', s.scheme_name,
                            'isin', s.isin,
                            'internal_scheme_code', s.internal_scheme_code,
                            'mapping_status', fsm.status
                        )
                    ) AS scheme_list
                FROM investment.folioschememapping fsm
                JOIN investment.masterscheme s ON s.scheme_id = fsm.scheme_id
                GROUP BY fsm.folio_id
            ),
            clearing AS (
                SELECT account_id, STRING_AGG(code_type || ':' || code_value, ', ') AS clearing_codes
                FROM public.masterclearingcode GROUP BY account_id
            ),
            sub_ac AS (
                SELECT ba.account_number, ba.account_nickname, ba.account_id,
                       b.bank_name, COALESCE(e.entity_name, ec.entity_name) AS entity_name,
                       cl.clearing_codes
                FROM public.masterbankaccount ba
                LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
                LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
                LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
                LEFT JOIN clearing cl ON cl.account_id = ba.account_id
            ),
            red_ac AS (
                SELECT ba.account_number, ba.account_nickname, ba.account_id,
                       b.bank_name, COALESCE(e.entity_name, ec.entity_name) AS entity_name,
                       cl.clearing_codes
                FROM public.masterbankaccount ba
                LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
                LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
                LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
                LEFT JOIN clearing cl ON cl.account_id = ba.account_id
            )
            SELECT
                m.folio_id, m.entity_name, m.old_entity_name, m.amc_name, m.old_amc_name,
                m.folio_number, m.old_folio_number, m.first_holder_name, m.old_first_holder_name,
                m.default_subscription_account, m.old_default_subscription_account,
                m.default_redemption_account, m.old_default_redemption_account,
                m.status, m.old_status, m.source, m.old_source, m.is_deleted,

                COALESCE(l.actiontype,'') AS action_type,
                COALESCE(l.processing_status,'') AS processing_status,
                COALESCE(l.action_id::text,'') AS action_id,
                COALESCE(l.requested_by,'') AS requested_by,
                TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
                COALESCE(l.checker_by,'') AS checker_by,
                TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
                COALESCE(l.checker_comment,'') AS checker_comment,
                COALESCE(l.reason,'') AS reason,

                COALESCE(h.created_by,'') AS created_by, COALESCE(h.created_at,'') AS created_at,
                COALESCE(h.edited_by,'') AS edited_by, COALESCE(h.edited_at,'') AS edited_at,
                COALESCE(h.deleted_by,'') AS deleted_by, COALESCE(h.deleted_at,'') AS deleted_at,

                sub.account_number AS sub_account_number, sub.account_nickname AS sub_account_nickname,
                sub.bank_name AS sub_bank_name, sub.entity_name AS sub_entity_name,
                sub.clearing_codes AS sub_clearing_codes,

                red.account_number AS red_account_number, red.account_nickname AS red_account_nickname,
                red.bank_name AS red_bank_name, red.entity_name AS red_entity_name,
                red.clearing_codes AS red_clearing_codes,

                COALESCE(sch.scheme_list, '[]') AS schemes

            FROM investment.masterfolio m
            LEFT JOIN latest_audit l ON l.folio_id = m.folio_id
            LEFT JOIN history h ON h.folio_id = m.folio_id
            LEFT JOIN sub_ac sub ON sub.account_number = m.default_subscription_account
            LEFT JOIN red_ac red ON red.account_number = m.default_redemption_account
            LEFT JOIN schemes sch ON sch.folio_id = m.folio_id

            WHERE COALESCE(m.is_deleted,false)=false
              AND m.folio_id = $1

		ORDER BY m.entity_name, m.folio_number;
        `

		rows, err := pgxPool.Query(ctx, q, req.FolioID)
		if err != nil {
			msg, status := getUserFriendlyFolioError(err, "query")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1)

		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{})
			for i, f := range fields {
				name := string(f.Name)
				if vals[i] == nil {
					rec[name] = ""
				} else if t, ok := vals[i].(time.Time); ok {
					rec[name] = t.Format(constants.DateTimeFormat)
				} else {
					rec[name] = vals[i]
				}
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScanFailedPrefix+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// GetSchemesByApprovedFolios returns schemes linked to approved and active folios/demats
// with approved and active bank accounts
func GetSchemesByApprovedFolios(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			WITH latest_folio_audit AS (
				SELECT DISTINCT ON (folio_id)
					folio_id,
					processing_status
				FROM investment.auditactionfolio
				ORDER BY folio_id, requested_at DESC
			),
			latest_scheme_audit AS (
				SELECT DISTINCT ON (scheme_id)
					scheme_id,
					processing_status
				FROM investment.auditactionscheme
				ORDER BY scheme_id, requested_at DESC
			),
			latest_demat_audit AS (
				SELECT DISTINCT ON (demat_id)
					demat_id,
					processing_status
				FROM investment.auditactiondemat
				ORDER BY demat_id, requested_at DESC
			),
			latest_bank_audit AS (
				SELECT DISTINCT ON (account_id)
					account_id,
					processing_status
				FROM public.auditactionbankaccount
				ORDER BY account_id, requested_at DESC
			),
			-- Active approved bank accounts with all possible identifiers
			approved_active_bank_accounts AS (
				SELECT 
					ba.account_number, 
					ba.account_id,
					ba.account_nickname
				FROM public.masterbankaccount ba
				LEFT JOIN latest_bank_audit lba ON lba.account_id = ba.account_id
				WHERE 
					(UPPER(COALESCE(lba.processing_status, 'APPROVED')) = 'APPROVED')
					AND UPPER(COALESCE(ba.status, 'Active')) = 'ACTIVE'
			),
			-- Approved active folios with valid bank accounts (flexible matching)
			approved_active_folios AS (
				SELECT m.folio_id
				FROM investment.masterfolio m
				JOIN latest_folio_audit lfa ON lfa.folio_id = m.folio_id
				-- Check subscription account is approved/active (flexible match on account_number, account_id, or account_nickname)
				JOIN approved_active_bank_accounts sub_ba ON (
					sub_ba.account_number = m.default_subscription_account
					OR sub_ba.account_id::text = m.default_subscription_account
					OR sub_ba.account_nickname = m.default_subscription_account
				)
				-- Check redemption account is approved/active (flexible match)
				JOIN approved_active_bank_accounts red_ba ON (
					red_ba.account_number = m.default_redemption_account
					OR red_ba.account_id::text = m.default_redemption_account
					OR red_ba.account_nickname = m.default_redemption_account
				)
				WHERE 
					UPPER(lfa.processing_status) = 'APPROVED'
					AND UPPER(m.status) = 'ACTIVE'
					AND COALESCE(m.is_deleted, false) = false
			),
			-- Approved active demats with valid bank accounts (flexible matching)
			approved_active_demats AS (
				SELECT d.demat_id, d.demat_account_number
				FROM investment.masterdemataccount d
				JOIN latest_demat_audit lda ON lda.demat_id = d.demat_id
				-- Check default settlement account is approved/active (flexible match)
				JOIN approved_active_bank_accounts ba ON (
					ba.account_number = d.default_settlement_account
					OR ba.account_id::text = d.default_settlement_account
					OR ba.account_nickname = d.default_settlement_account
				)
				WHERE 
					UPPER(lda.processing_status) = 'APPROVED'
					AND UPPER(d.status) = 'ACTIVE'
					AND COALESCE(d.is_deleted, false) = false
			),
			-- Approved active schemes
			approved_active_schemes AS (
				SELECT s.scheme_id
				FROM investment.masterscheme s
				JOIN latest_scheme_audit lsa ON lsa.scheme_id = s.scheme_id
				WHERE 
					UPPER(lsa.processing_status) = 'APPROVED'
					AND UPPER(s.status) = 'ACTIVE'
					AND COALESCE(s.is_deleted, false) = false
			),
			-- Schemes from folio-scheme mapping
			schemes_from_folio AS (
				SELECT DISTINCT fsm.scheme_id
				FROM investment.folioschememapping fsm
				JOIN approved_active_folios aaf ON aaf.folio_id = fsm.folio_id
				JOIN approved_active_schemes aas ON aas.scheme_id = fsm.scheme_id
				WHERE COALESCE(fsm.status, 'Active') = 'Active'
			),
			-- Schemes from demat via portfolio_snapshot
			schemes_from_demat AS (
				SELECT DISTINCT ps.scheme_id
				FROM investment.portfolio_snapshot ps
				JOIN approved_active_demats aad ON aad.demat_account_number = ps.demat_acc_number
				JOIN approved_active_schemes aas ON aas.scheme_id = ps.scheme_id
				WHERE ps.total_units > 0
			),
			-- Combined schemes from both folio and demat
			all_schemes AS (
				SELECT scheme_id FROM schemes_from_folio
				UNION
				SELECT scheme_id FROM schemes_from_demat
			)
			SELECT DISTINCT
				s.scheme_id,
				s.scheme_name,
				COALESCE(s.isin, '') AS isin,
				COALESCE(s.internal_scheme_code, '') AS internal_scheme_code,
				COALESCE(s.amfi_scheme_code, '') AS amfi_code
			FROM all_schemes als
			JOIN investment.masterscheme s ON s.scheme_id = als.scheme_id
		ORDER BY s.scheme_name;
	`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlyFolioError(err, "query")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var schemeID, schemeName, isin, internalCode, amfiCode string
			if err := rows.Scan(&schemeID, &schemeName, &isin, &internalCode, &amfiCode); err != nil {
				msg, status := getUserFriendlyFolioError(err, "scan")
				api.RespondWithError(w, status, msg)
				return
			}
			out = append(out, map[string]interface{}{
				"scheme_id":            schemeID,
				"scheme_name":          schemeName,
				"isin":                 isin,
				"internal_scheme_code": internalCode,
				"amfi_code":            amfiCode,
			})
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsError+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}
