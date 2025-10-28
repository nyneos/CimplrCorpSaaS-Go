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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func UploadFolio(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startOverall := time.Now()
		ctx := r.Context()
		timings := []map[string]interface{}{}

		userID := r.FormValue("user_id")
		if userID == "" {
			var tmp struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&tmp)
			userID = tmp.UserID
		}
		if userID == "" {
			api.RespondWithError(w, 400, "user_id required")
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
			api.RespondWithError(w, 401, "invalid session")
			return
		}

		if err := r.ParseMultipartForm(64 << 20); err != nil {
			api.RespondWithError(w, 400, "form parse: "+err.Error())
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, 400, "no file uploaded")
			return
		}

		results := []map[string]interface{}{}

		for _, fh := range files {
			fileStart := time.Now()
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, 400, "open file: "+err.Error())
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

			schemeMap := map[string]string{}
			if len(schemeRefs) > 0 {
				refList := make([]string, 0, len(schemeRefs))
				for r := range schemeRefs {
					refList = append(refList, r)
				}
				rows, err := pgxPool.Query(ctx, `
					SELECT scheme_id, scheme_name, isin, internal_scheme_code
					FROM investment.masterscheme
					WHERE COALESCE(is_deleted,false)=false
					  AND (scheme_id = ANY($1) OR scheme_name = ANY($1) OR isin = ANY($1) OR internal_scheme_code = ANY($1))
				`, refList)
				if err == nil {
					for rows.Next() {
						var sid, sname, isin, icode string
						_ = rows.Scan(&sid, &sname, &isin, &icode)
						for _, k := range []string{sid, sname, isin, icode} {
							if k != "" {
								schemeMap[strings.TrimSpace(k)] = sid
							}
						}
					}
					rows.Close()
				}
			}

			copyCols := []string{
				"entity_name", "amc_name", "folio_number", "first_holder_name",
				"default_subscription_account", "default_redemption_account", "status", "source",
			}
			copyRows := make([][]interface{}, 0, len(dataRows))
			for _, r := range dataRows {
				row := make([]interface{}, len(copyCols))
				for j, c := range copyCols {
					switch c {
					case "status":
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
				api.RespondWithError(w, 500, "tx: "+err.Error())
				return
			}
			defer tx.Rollback(ctx)
			_, _ = tx.Exec(ctx, "SET LOCAL synchronous_commit TO OFF")

			_, err = tx.Exec(ctx, `
				DROP TABLE IF EXISTS tmp_folio;
				CREATE TEMP TABLE tmp_folio (LIKE investment.masterfolio INCLUDING DEFAULTS) ON COMMIT DROP;
			`)
			if err != nil {
				api.RespondWithError(w, 500, "tmp table: "+err.Error())
				return
			}

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_folio"}, copyCols, pgx.CopyFromRows(copyRows)); err != nil {
				api.RespondWithError(w, 500, "copy: "+err.Error())
				return
			}

			upsertSQL := `
				INSERT INTO investment.masterfolio (
					entity_name, amc_name, folio_number, first_holder_name,
					default_subscription_account, default_redemption_account,
					status, source
				)
				SELECT entity_name, amc_name, folio_number, first_holder_name,
					   default_subscription_account, default_redemption_account,
					   COALESCE(status,'Active'), COALESCE(source,'Upload')
				FROM tmp_folio
				ON CONFLICT (entity_name, amc_name, folio_number)
				DO UPDATE SET
					entity_name = EXCLUDED.entity_name,
					amc_name = EXCLUDED.amc_name,
					first_holder_name = EXCLUDED.first_holder_name,
					default_subscription_account = EXCLUDED.default_subscription_account,
					default_redemption_account = EXCLUDED.default_redemption_account,
					status = EXCLUDED.status,
					source = EXCLUDED.source;
			`
			if _, err := tx.Exec(ctx, upsertSQL); err != nil {
				api.RespondWithError(w, 500, "upsert masterfolio: "+err.Error())
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
				api.RespondWithError(w, 500, "audit insert: "+err.Error())
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
					api.RespondWithError(w, 500, "mapping tmp: "+err.Error())
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
						[]string{"folio_number", "scheme_id", "status"},
						pgx.CopyFromRows(mappingRows)); err != nil {
						api.RespondWithError(w, 500, "mapping copy: "+err.Error())
						return
					}

					if _, err := tx.Exec(ctx, `
						INSERT INTO investment.folioschememapping (folio_id, scheme_id, status)
						SELECT m.folio_id, t.scheme_id, t.status
						FROM tmp_folio_scheme t
						JOIN investment.masterfolio m ON m.folio_number = t.folio_number
						ON CONFLICT (folio_id, scheme_id) DO NOTHING;
					`); err != nil {
						api.RespondWithError(w, 500, "mapping insert: "+err.Error())
						return
					}
				}
			}
			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, 500, "commit: "+err.Error())
				return
			}

			results = append(results, map[string]interface{}{
				"batch_id": uuid.New().String(),
				"rows":     len(copyRows),
				"success":  true,
				"elapsed":  time.Since(fileStart).Milliseconds(),
			})
		}

		total := time.Since(startOverall).Milliseconds()
		api.RespondWithPayload(w, true, "", map[string]any{
			"success":    true,
			"processed":  len(results),
			"batches":    results,
			"total_ms":   total,
			"rows_count": len(results),
		})
	}
}

func GetFoliosWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

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
			LEFT JOIN sub_ac sub ON sub.account_number = m.default_subscription_account
			LEFT JOIN red_ac red ON red.account_number = m.default_redemption_account
			LEFT JOIN schemes sch ON sch.folio_id = m.folio_id
			WHERE COALESCE(m.is_deleted,false)=false
			ORDER BY m.entity_name, m.folio_number;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
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
					rec[name] = t.Format("2006-01-02 15:04:05")
				} else {
					rec[name] = vals[i]
				}
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "scan failed: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func GetApprovedActiveFolios(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

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
			ORDER BY m.entity_name, m.folio_number;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "scan failed: "+rows.Err().Error())
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		// required
		if strings.TrimSpace(req.EntityName) == "" || strings.TrimSpace(req.AMCName) == "" ||
			strings.TrimSpace(req.FolioNumber) == "" || strings.TrimSpace(req.FirstHolderName) == "" ||
			strings.TrimSpace(req.DefaultSubscriptionAcct) == "" || strings.TrimSpace(req.DefaultRedemptionAcct) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing required fields")
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// uniqueness check
		var existing string
		err = tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE entity_name=$1 AND amc_name=$2 AND folio_number=$3 AND COALESCE(is_deleted,false)=false LIMIT 1`,
			req.EntityName, req.AMCName, req.FolioNumber).Scan(&existing)
		if err == nil {
			api.RespondWithError(w, http.StatusBadRequest, "Folio already exists")
			return
		}

		var folioID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.masterfolio
				(entity_name, amc_name, folio_number, first_holder_name, default_subscription_account, default_redemption_account, status, source)
			VALUES ($1,$2,$3,$4,$5,$6,$7,'Manual')
			RETURNING folio_id
		`, req.EntityName, req.AMCName, req.FolioNumber, req.FirstHolderName, req.DefaultSubscriptionAcct, req.DefaultRedemptionAcct, defaultIfEmpty(req.Status, "Active")).Scan(&folioID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}

		// resolve scheme refs (if provided) to scheme_ids
		if len(req.SchemeRefs) > 0 {
			// build unique list
			refMap := map[string]struct{}{}
			for _, r := range req.SchemeRefs {
				if rr := strings.TrimSpace(r); rr != "" {
					refMap[rr] = struct{}{}
				}
			}
			refList := make([]string, 0, len(refMap))
			for k := range refMap {
				refList = append(refList, k)
			}
			if len(refList) > 0 {
				rows, err := tx.Query(ctx, `
					SELECT scheme_id, scheme_name, isin, internal_scheme_code
					FROM investment.masterscheme
					WHERE scheme_id = ANY($1) OR scheme_name = ANY($1) OR isin = ANY($1) OR internal_scheme_code = ANY($1)
				`, refList)
				if err == nil {
					defer rows.Close()
					mappingRows := [][]interface{}{}
					for rows.Next() {
						var sid, sname, isin, icode string
						_ = rows.Scan(&sid, &sname, &isin, &icode)
						// Insert mapping for folio <-> sid
						mappingRows = append(mappingRows, []interface{}{folioID, sid, "Active", nil})
					}
					if len(mappingRows) > 0 {
						if _, err := tx.CopyFrom(ctx, pgx.Identifier{"investment", "folioschememapping"},
							[]string{"folio_id", "scheme_id", "status", "old_status"}, pgx.CopyFromRows(mappingRows)); err != nil {
							api.RespondWithError(w, http.StatusInternalServerError, "folio-scheme mapping failed: "+err.Error())
							return
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
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No rows provided")
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "tx begin failed: " + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			// validations
			if strings.TrimSpace(row.EntityName) == "" || strings.TrimSpace(row.AMCName) == "" ||
				strings.TrimSpace(row.FolioNumber) == "" || strings.TrimSpace(row.FirstHolderName) == "" ||
				strings.TrimSpace(row.DefaultSubscriptionAcct) == "" || strings.TrimSpace(row.DefaultRedemptionAcct) == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "missing required fields"})
				continue
			}

			// uniqueness
			var exists string
			err = tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE entity_name=$1 AND amc_name=$2 AND folio_number=$3 AND COALESCE(is_deleted,false)=false LIMIT 1`,
				row.EntityName, row.AMCName, row.FolioNumber).Scan(&exists)
			if err == nil {
				results = append(results, map[string]interface{}{"success": false, "entity": row.EntityName, "folio_number": row.FolioNumber, "error": "duplicate"})
				continue
			}

			var folioID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.masterfolio
					(entity_name, amc_name, folio_number, first_holder_name, default_subscription_account, default_redemption_account, status, source)
				VALUES ($1,$2,$3,$4,$5,$6,$7,'Manual')
				RETURNING folio_id
			`, row.EntityName, row.AMCName, row.FolioNumber, row.FirstHolderName, row.DefaultSubscriptionAcct, row.DefaultRedemptionAcct, defaultIfEmpty(row.Status, "Active")).Scan(&folioID); err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "insert failed: " + err.Error()})
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
								[]string{"folio_id", "scheme_id", "status", "old_status"}, pgx.CopyFromRows(mappingRows)); err != nil {
								results = append(results, map[string]interface{}{"success": false, "error": "mapping insert failed: " + err.Error()})
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
				results = append(results, map[string]interface{}{"success": false, "error": "audit insert failed: " + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "commit failed: " + err.Error()})
				continue
			}
			results = append(results, map[string]interface{}{"success": true, "folio_id": folioID})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if strings.TrimSpace(req.FolioID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "folio_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no fields to update")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"entity_name":                  0,
			"amc_name":                     1,
			"folio_number":                 2,
			"first_holder_name":            3,
			"default_subscription_account": 4,
			"default_redemption_account":   5,
			"status":                       6,
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
				sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", lk, pos, oldField, pos+1))
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
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
		`, req.FolioID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if row.FolioID == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "folio_id missing"})
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "folio_id": row.FolioID, "error": "tx begin failed: " + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			var oldVals [7]interface{}
			if err := tx.QueryRow(ctx, `
				SELECT entity_name, amc_name, folio_number, first_holder_name,
				       default_subscription_account, default_redemption_account, status
				FROM investment.masterfolio WHERE folio_id=$1 FOR UPDATE
			`, row.FolioID).Scan(&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4], &oldVals[5], &oldVals[6]); err != nil {
				results = append(results, map[string]interface{}{"success": false, "folio_id": row.FolioID, "error": "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"entity_name":                  0,
				"amc_name":                     1,
				"folio_number":                 2,
				"first_holder_name":            3,
				"default_subscription_account": 4,
				"default_redemption_account":   5,
				"status":                       6,
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
								results = append(results, map[string]interface{}{"success": false, "folio_id": row.FolioID, "error": "folio_number already exists"})
								continue
							}
						}
					}
					oldField := "old_" + lk
					sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", lk, pos, oldField, pos+1))
					args = append(args, v, oldVals[idx])
					pos += 2
				}
			}
			if len(sets) == 0 {
				results = append(results, map[string]interface{}{"success": false, "folio_id": row.FolioID, "error": "No valid fields"})
				continue
			}
			q := fmt.Sprintf("UPDATE investment.masterfolio SET %s WHERE folio_id=$%d", strings.Join(sets, ", "), pos)
			args = append(args, row.FolioID)
			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{"success": false, "folio_id": row.FolioID, "error": "update failed: " + err.Error()})
				continue
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
			`, row.FolioID, row.Reason, userEmail); err != nil {
				results = append(results, map[string]interface{}{"success": false, "folio_id": row.FolioID, "error": "audit insert failed: " + err.Error()})
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{"success": false, "folio_id": row.FolioID, "error": "commit failed: " + err.Error()})
				continue
			}
			results = append(results, map[string]interface{}{"success": true, "folio_id": row.FolioID})
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.FolioIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())
			`, id, req.Reason, requestedBy); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "insert failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
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
			api.RespondWithPayload(w, false, "No approvable actions found", map[string]any{
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
				api.RespondWithError(w, http.StatusInternalServerError, "approve update failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionfolio
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "mark deleted failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE investment.masterfolio
				SET is_deleted=true, status='Inactive'
				WHERE folio_id = ANY($1)
			`, deleteMasterIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "master soft-delete failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
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
				msg += fmt.Sprintf("no audit action found for folio_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved folio_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactionfolio
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE action_id = ANY($3)
		`, checkerBy, req.Comment, actionIDs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}
