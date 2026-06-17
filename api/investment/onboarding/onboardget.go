package investment

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/ctxutil"
	"CimplrCorpSaas/internal/validation"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func validateOnboardingLookupRows(w http.ResponseWriter, r *http.Request, rows []map[string]interface{}) bool {
	for _, row := range rows {
		if msg := validation.ValidateMFMasterReferences(r.Context(), row); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return false
		}
	}
	return true
}

func GetAMFISchemeAMCEnriched(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		query := `
			WITH distinct_amfi AS (
				SELECT DISTINCT amc_name
				FROM investment.amfi_scheme_master_staging
				WHERE amc_name IS NOT NULL
				  AND amc_name <> ''
			)
			SELECT 
				a.amc_name,
				COALESCE(m.amc_id, '') AS amc_id,
				CASE 
					WHEN m.amc_id IS NOT NULL AND m.is_deleted = false THEN true
					ELSE false
				END AS enriched
			FROM distinct_amfi a
			LEFT JOIN investment.masteramc m 
				ON LOWER(TRIM(a.amc_name)) = LOWER(TRIM(m.amc_name))
			ORDER BY a.amc_name;
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var out []map[string]interface{}
		for rows.Next() {
			var amcName, amcID string
			var enriched bool
			_ = rows.Scan(&amcName, &amcID, &enriched)
			out = append(out, map[string]interface{}{
				"amc_name": amcName,
				"amc_id":   amcID,
				"enriched": enriched,
			})
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func GetAMFISchemesByMultipleAMCs(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var req struct {
			AMCs []string `json:"amcs"` // multiple AMC names or IDs
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if len(req.AMCs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Missing field: amcs (array of AMC names or IDs)")
			return
		}

		// Normalise input (trim + lower) — no approval check needed here;
		// this is a read-only AMFI staging lookup used during onboarding where
		// the AMC may not be approved yet.
		for i, a := range req.AMCs {
			req.AMCs[i] = strings.ToLower(strings.TrimSpace(a))
		}

		query := `
			WITH input_amcs AS (
				SELECT unnest($1::text[]) AS amc_key
			),
			target_amcs AS (
				SELECT DISTINCT amc_name
				FROM investment.masteramc m
				JOIN input_amcs i
					ON LOWER(TRIM(m.amc_name)) = i.amc_key
					OR LOWER(TRIM(m.amc_id)) = i.amc_key
				WHERE m.is_deleted = false
				UNION
				SELECT DISTINCT s.amc_name
				FROM investment.amfi_scheme_master_staging s
				JOIN input_amcs i
					ON LOWER(TRIM(s.amc_name)) = i.amc_key
			)
			SELECT
				s.amc_name,
				COALESCE(s.scheme_nav_name, s.scheme_name) AS scheme_name,
				'' AS internal_scheme_code,
				COALESCE(s.isin_div_payout_growth, '') AS isin,
				COALESCE(s.isin_div_reinvestment, '') AS isin_reinvest,
				s.scheme_code,
				CASE
					WHEN m.scheme_id IS NOT NULL AND COALESCE(m.is_deleted, false) = false THEN true
					ELSE false
				END AS enriched
			FROM investment.amfi_scheme_master_staging s
			JOIN target_amcs a
				ON LOWER(TRIM(s.amc_name)) = LOWER(TRIM(a.amc_name))
			LEFT JOIN investment.masterscheme m
				ON (
					(COALESCE(s.isin_div_payout_growth,'') <> '' AND LOWER(TRIM(s.isin_div_payout_growth)) = LOWER(TRIM(m.isin)))
					OR m.amfi_scheme_code = s.scheme_code::text
				)
				AND COALESCE(m.is_deleted, false) = false
			ORDER BY s.amc_name, scheme_name;
		`

		rows, err := pgxPool.Query(ctx, query, req.AMCs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var amcName, schemeName, internalSchemeCode, isin, isinReinvest string
			var schemeCode int64
			var enriched bool
			_ = rows.Scan(&amcName, &schemeName, &internalSchemeCode, &isin, &isinReinvest, &schemeCode, &enriched)
			out = append(out, map[string]interface{}{
				"amc_name":             amcName,
				"scheme_name":          schemeName,
				"isin":                 isin,
				"isin_reinvest":        isinReinvest,
				"scheme_code":          schemeCode,
				"enriched":             enriched,
				"internal_scheme_code": internalSchemeCode,
			})
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func GetFoliosBySchemeListSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var req struct {
			Schemes []string `json:"schemes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Schemes) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No schemes provided")
			return
		}

		// Clean input names
		for i, s := range req.Schemes {
			req.Schemes[i] = strings.TrimSpace(s)
		}

		query := `
			WITH input_schemes AS (
				SELECT unnest($1::text[]) AS scheme_name
			),
			matched_master AS (
				SELECT 
					ms.scheme_name,
					mf.folio_number,
					mf.entity_name,
					mf.amc_name,
					COALESCE(mf.status, '') AS status,
					true AS enriched
				FROM investment.masterscheme ms
				JOIN investment.folioschememapping fsm ON fsm.scheme_id = ms.scheme_id
				JOIN investment.masterfolio mf ON mf.folio_id = fsm.folio_id
				JOIN input_schemes i 
					ON LOWER(TRIM(ms.scheme_name)) = LOWER(TRIM(i.scheme_name))
				WHERE 
					COALESCE(ms.is_deleted, false) = false
					AND COALESCE(mf.is_deleted, false) = false
			),
			amfi_fallback AS (
				SELECT 
					i.scheme_name,
					'' AS folio_number,
					'' AS entity_name,
					COALESCE(s.amc_name, '') AS amc_name,
					'' AS status,
					false AS enriched
				FROM input_schemes i
				LEFT JOIN investment.amfi_scheme_master_staging s 
					ON LOWER(TRIM(s.scheme_name)) = LOWER(TRIM(i.scheme_name))
				WHERE NOT EXISTS (
					SELECT 1 
					FROM matched_master m 
					WHERE LOWER(TRIM(m.scheme_name)) = LOWER(TRIM(i.scheme_name))
				)
			)
			SELECT * FROM matched_master
			UNION ALL
			SELECT * FROM amfi_fallback
			ORDER BY scheme_name;
		`

		rows, err := pgxPool.Query(ctx, query, req.Schemes)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var schemeName, folioNumber, entityName, amcName, status string
			var enriched bool
			_ = rows.Scan(&schemeName, &folioNumber, &entityName, &amcName, &status, &enriched)
			out = append(out, map[string]interface{}{
				"scheme_name":       schemeName,
				"folio_number":      folioNumber,
				"entity_name":       entityName,
				"amc_name":          amcName,
				constants.KeyStatus: status,
				"enriched":          enriched,
			})
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func GetFoliosBySchemeListGrouped(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var req struct {
			Schemes []string `json:"schemes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Schemes) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No schemes provided")
			return
		}

		// clean scheme names
		for i, s := range req.Schemes {
			req.Schemes[i] = strings.TrimSpace(s)
		}

		query := `
			WITH input_schemes AS (
				SELECT unnest($1::text[]) AS scheme_name
			),
			matched_master AS (
				SELECT 
					ms.scheme_name,
					mf.folio_number,
					mf.entity_name,
					mf.amc_name,
					COALESCE(mf.status, '') AS status,
					true AS enriched
				FROM investment.masterscheme ms
				JOIN investment.folioschememapping fsm ON fsm.scheme_id = ms.scheme_id
				JOIN investment.masterfolio mf ON mf.folio_id = fsm.folio_id
				JOIN input_schemes i 
					ON LOWER(TRIM(ms.scheme_name)) = LOWER(TRIM(i.scheme_name))
				WHERE 
					COALESCE(ms.is_deleted, false) = false
					AND COALESCE(mf.is_deleted, false) = false
			),
			amfi_fallback AS (
				SELECT 
					i.scheme_name,
					'' AS folio_number,
					'' AS entity_name,
					COALESCE(s.amc_name, '') AS amc_name,
					'' AS status,
					false AS enriched
				FROM input_schemes i
				LEFT JOIN investment.amfi_scheme_master_staging s 
					ON LOWER(TRIM(s.scheme_name)) = LOWER(TRIM(i.scheme_name))
				WHERE NOT EXISTS (
					SELECT 1 
					FROM matched_master m 
					WHERE LOWER(TRIM(m.scheme_name)) = LOWER(TRIM(i.scheme_name))
				)
			)
			SELECT * FROM matched_master
			UNION ALL
			SELECT * FROM amfi_fallback
			ORDER BY scheme_name;
		`

		rows, err := pgxPool.Query(ctx, query, req.Schemes)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		grouped := make(map[string][]map[string]interface{})

		for rows.Next() {
			var schemeName, folioNumber, entityName, amcName, status string
			var enriched bool
			_ = rows.Scan(&schemeName, &folioNumber, &entityName, &amcName, &status, &enriched)

			grouped[schemeName] = append(grouped[schemeName], map[string]interface{}{
				"folio_number":      folioNumber,
				"entity_name":       entityName,
				"amc_name":          amcName,
				constants.KeyStatus: status,
				"enriched":          enriched,
			})
		}

		api.RespondWithPayload(w, true, "", grouped)
	}
}

// GetDematWithDPInfo returns combined Demat Account + Depository Participant info
func GetDematWithDPInfo(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			SELECT
				dm.demat_id,
				dm.entity_name,
				dm.dp_id,
				dm.depository,
				dm.demat_account_number,
				dm.depository_participant,
				dm.client_id,
				dm.default_settlement_account,
				dm.status AS demat_status,
				dm.source AS demat_source,
				dm.is_deleted AS demat_deleted,

				dp.dp_name,
				dp.dp_code,
				dp.depository AS dp_depository,
				dp.status AS dp_status,
				dp.source AS dp_source,
				dp.is_deleted AS dp_deleted
			FROM investment.masterdemataccount dm
			LEFT JOIN investment.masterdepositoryparticipant dp
				ON dp.dp_id = dm.dp_id
			WHERE 
				COALESCE(dm.is_deleted, false) = false
				AND COALESCE(dp.is_deleted, false) = false
			ORDER BY dm.entity_name, dm.demat_account_number;
		`
		args := []interface{}{}
		if scope := ctxutil.FromContext(ctx); len(scope.EntityNames) > 0 {
			q = strings.Replace(q, "ORDER BY dm.entity_name", "AND dm.entity_name = ANY($1::text[])\n\t\t\tORDER BY dm.entity_name", 1)
			args = append(args, scope.EntityNames)
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var out []map[string]interface{}
		for rows.Next() {
			var (
				dematID, entityName, dpID, depository, dematAccNum, defaultAcc, dematStatus, dematSource string
				depositoryParticipant, clientID                                                          *string
				dpName, dpCode, dpDepository, dpStatus, dpSource                                         *string
				dematDeleted, dpDeleted                                                                  *bool
			)

			if err := rows.Scan(
				&dematID, &entityName, &dpID, &depository, &dematAccNum, &depositoryParticipant,
				&clientID, &defaultAcc, &dematStatus, &dematSource, &dematDeleted,
				&dpName, &dpCode, &dpDepository, &dpStatus, &dpSource, &dpDeleted,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScanFailedPrefix+err.Error())
				return
			}

			out = append(out, map[string]interface{}{
				// 🔹 Demat Info
				"demat_id":                   dematID,
				"entity_name":                entityName,
				"dp_id":                      dpID,
				"depository":                 depository,
				"demat_account_number":       dematAccNum,
				"depository_participant":     ifNotNil(depositoryParticipant),
				"client_id":                  ifNotNil(clientID),
				"default_settlement_account": defaultAcc,
				"demat_status":               dematStatus,
				"demat_source":               dematSource,
				"demat_deleted":              ifNotBool(dematDeleted),

				// 🔹 DP Info
				"dp_name":       ifNotNil(dpName),
				"dp_code":       ifNotNil(dpCode),
				"dp_depository": ifNotNil(dpDepository),
				"dp_status":     ifNotNil(dpStatus),
				"dp_source":     ifNotNil(dpSource),
				"dp_deleted":    ifNotBool(dpDeleted),
			})
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "rows iteration failed: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// GetFoliosByEntity returns all existing folios for a given entity (and optional AMC),
// regardless of which schemes are mapped — any scheme can be added to an existing folio.
func GetFoliosByEntity(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var req struct {
			EntityName string `json:"entity_name"`
			AmcName    string `json:"amc_name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.EntityName == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_name is required")
			return
		}

		// Use $2 = '' trick to make amc_name filter optional without fmt
		query := `
			SELECT
				mf.folio_id,
				mf.folio_number,
				mf.entity_name,
				COALESCE(mf.amc_name, '') AS amc_name,
				COALESCE(mf.status, '') AS status
			FROM investment.masterfolio mf
			WHERE COALESCE(mf.is_deleted, false) = false
			  AND mf.entity_name = $1
			  AND ($2 = '' OR LOWER(TRIM(mf.amc_name)) = LOWER(TRIM($2)))
			ORDER BY mf.amc_name, mf.folio_number;
		`

		rows, err := pgxPool.Query(ctx, query, req.EntityName, req.AmcName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var folioID, folioNumber, entityName, amcName, status string
			if err := rows.Scan(&folioID, &folioNumber, &entityName, &amcName, &status); err != nil {
				continue
			}
			out = append(out, map[string]interface{}{
				"folio_id":     folioID,
				"folio_number": folioNumber,
				"entity_name":  entityName,
				"amc_name":     amcName,
				"status":       status,
			})
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// helper for nil-safe string conversion
func ifNotNil(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// helper for nil-safe bool conversion
func ifNotBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

// GetAllDPs returns all Depository Participants where is_deleted = false (no approval/active filters)
func GetAllDPs(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			SELECT 
				dp_id,
				dp_name,
				dp_code,
				depository,
				status,
				source,
				is_deleted
			FROM investment.masterdepositoryparticipant
			WHERE COALESCE(is_deleted, false) = false
			ORDER BY dp_name;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var dpID, dpName, dpCode, depository, status string
			var source *string
			var isDeleted bool

			if err := rows.Scan(&dpID, &dpName, &dpCode, &depository, &status, &source, &isDeleted); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScanFailedPrefix+err.Error())
				return
			}

			out = append(out, map[string]interface{}{
				"dp_id":             dpID,
				"dp_name":           dpName,
				"dp_code":           dpCode,
				"depository":        depository,
				constants.KeyStatus: status,
				"source":            ifNotNil(source),
				"is_deleted":        isDeleted,
			})
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "rows iteration failed: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}
