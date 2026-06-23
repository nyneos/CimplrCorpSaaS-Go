package portfolio

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/schemejoin"
	"CimplrCorpSaas/internal/ctxutil"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TxFilter struct {
	EntityName string `json:"entity_name"`
	AmcName    string `json:"amc_name"`
	SchemeName string `json:"scheme_name"`
	TxType     string `json:"tx_type"` // "Investment" | "Redemption" | "Onboard" | ""
	Status     string `json:"status"`
	DateFrom   string `json:"date_from"` // YYYY-MM-DD
	DateTo     string `json:"date_to"`   // YYYY-MM-DD
}

// PortfolioTxRow is the unified row for investment confirmations,
// redemption confirmations, and onboard transactions.
type PortfolioTxRow struct {
	// Primary columns shown in the table
	TxType           string  `json:"tx_type"` // "Investment" | "Redemption" | "Onboard"
	EntityName       string  `json:"entity_name"`
	SchemeName       string  `json:"scheme_name"`
	AmfiSchemeCode   string  `json:"amfi_scheme_code"`
	Amount           float64 `json:"amount"`
	Status           string  `json:"status"`
	ProcessingStatus string  `json:"processing_status"`

	// Detail columns shown in the expandable row
	AmcName         string  `json:"amc_name"`
	SchemeCode      string  `json:"scheme_code"` // internal_scheme_code
	ISIN            string  `json:"isin"`
	FolioNumber     string  `json:"folio_number"`
	DematNumber     string  `json:"demat_number"`
	TransactionDate string  `json:"transaction_date"`
	TransactionType string  `json:"transaction_type"` // Purchase/Sell for Onboard
	Units           float64 `json:"units"`
	Nav             float64 `json:"nav"`
	NavDate         string  `json:"nav_date"`
	NetAmount       float64 `json:"net_amount"`
	StampDuty       float64 `json:"stamp_duty"`
	ExitLoad        float64 `json:"exit_load"`
	TDS             float64 `json:"tds"`
	ConfirmedAt     string  `json:"confirmed_at"`
	Source          string  `json:"source"` // how it was created

	// IDs
	InitiationID   string `json:"initiation_id"`
	ConfirmationID string `json:"confirmation_id"`
	BatchID        string `json:"batch_id"`
	SchemeID       string `json:"scheme_id"`
	FolioID        string `json:"folio_id"`
	DematID        string `json:"demat_id"`
}

// paramSet tracks the positional index for each filter value.
// Index 0 means the filter is not active.
type paramSet struct {
	entityIdx   int
	scopeIdx    int // entity scope list (alternative to entityIdx)
	amcIdx      int
	schemeIdx   int
	statusIdx   int
	dateFromIdx int
	dateToIdx   int
}

func GetPortfolioTransactions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)

		var f TxFilter
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil && r.ContentLength > 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		f.EntityName = strings.TrimSpace(f.EntityName)
		f.AmcName = strings.TrimSpace(f.AmcName)
		f.SchemeName = strings.TrimSpace(f.SchemeName)
		f.TxType = strings.TrimSpace(f.TxType)
		f.Status = strings.TrimSpace(f.Status)
		f.DateFrom = strings.TrimSpace(f.DateFrom)
		f.DateTo = strings.TrimSpace(f.DateTo)

		if f.EntityName != "" && !scope.HasEntityNameAccess(f.EntityName) {
			api.RespondWithError(w, http.StatusForbidden, "entity_name is not within your authorized access scope")
			return
		}

		// Build args and record which positional index holds each filter.
		// All UNION legs share these same args, just referencing with different column names.
		var args []interface{}
		ps := paramSet{}
		add := func(val interface{}) string {
			args = append(args, val)
			return fmt.Sprintf("$%d", len(args))
		}
		ph := func(idx int) string { return fmt.Sprintf("$%d", idx) }

		if f.EntityName != "" {
			_ = add(f.EntityName)
			ps.entityIdx = len(args)
		} else if len(scope.EntityNames) > 0 {
			_ = add(scope.EntityNames)
			ps.scopeIdx = len(args)
		}
		if f.AmcName != "" {
			_ = add("%" + strings.ToLower(f.AmcName) + "%")
			ps.amcIdx = len(args)
		}
		if f.SchemeName != "" {
			_ = add("%" + strings.ToLower(f.SchemeName) + "%")
			ps.schemeIdx = len(args)
		}
		if f.Status != "" {
			_ = add(strings.ToUpper(f.Status))
			ps.statusIdx = len(args)
		}
		if f.DateFrom != "" {
			_ = add(f.DateFrom)
			ps.dateFromIdx = len(args)
		}
		if f.DateTo != "" {
			_ = add(f.DateTo)
			ps.dateToIdx = len(args)
		}

		// Build WHERE predicates for a given leg.
		// entityCol / amcCol / schemeCol / statusCol / dateCol differ per leg.
		buildWhere := func(entityCol, amcCol, schemeCol, statusCol, dateCol string, skipStatus bool) string {
			var parts []string
			trimmedEntity := "LOWER(TRIM(COALESCE(" + entityCol + ",'')))"
			if ps.entityIdx > 0 {
				parts = append(parts, trimmedEntity+"=LOWER(TRIM("+ph(ps.entityIdx)+"))")
			} else if ps.scopeIdx > 0 {
				parts = append(parts, trimmedEntity+"=ANY(SELECT LOWER(TRIM(x)) FROM unnest("+ph(ps.scopeIdx)+"::text[]) AS x)")
			}
			if ps.amcIdx > 0 {
				parts = append(parts, "LOWER(COALESCE("+amcCol+",'')) LIKE "+ph(ps.amcIdx))
			}
			if ps.schemeIdx > 0 {
				parts = append(parts, "LOWER(COALESCE("+schemeCol+",'')) LIKE "+ph(ps.schemeIdx))
			}
			if ps.statusIdx > 0 && !skipStatus {
				parts = append(parts, "UPPER(COALESCE("+statusCol+",''))="+ph(ps.statusIdx))
			}
			if ps.statusIdx > 0 && skipStatus {
				// Onboard rows have a fixed status; exclude them when filtering by status
				parts = append(parts, "1=0")
			}
			if ps.dateFromIdx > 0 {
				parts = append(parts, dateCol+">="+ph(ps.dateFromIdx)+"::date")
			}
			if ps.dateToIdx > 0 {
				parts = append(parts, dateCol+"<="+ph(ps.dateToIdx)+"::date")
			}
			if len(parts) == 0 {
				return ""
			}
			return " AND " + strings.Join(parts, " AND ")
		}

		schemeJoin := `LEFT JOIN LATERAL (
			SELECT ` + schemejoin.LateralSelectCols + `
			FROM investment.masterscheme
			WHERE ` + schemejoin.LateralWhereInitiationRef + `
			LIMIT 1
		) s ON true`
		folioJoin := `LEFT JOIN investment.masterfolio mf ON (mf.folio_id::text = i.folio_id OR mf.folio_number = i.folio_id)`
		dematJoin := `LEFT JOIN investment.masterdemataccount md ON (md.demat_id::text = i.demat_id OR md.demat_account_number = i.demat_id)`

		schemeNameInvest := `COALESCE(s.scheme_name,i.scheme_id,'')`
		schemeNameRedempt := `COALESCE(s.scheme_name,i.scheme_id,'')`
		schemeNameOnboard := `COALESCE(s.scheme_name,ot.scheme_internal_code,'')`
		amfiInvest := schemejoin.ResolveAMFICodeExpr("s", schemeNameInvest)
		amfiRedempt := schemejoin.ResolveAMFICodeExpr("s", schemeNameRedempt)
		amfiOnboard := schemejoin.ResolveAMFICodeExpr("s", schemeNameOnboard)

		investSQL := fmt.Sprintf(`
			SELECT
				'Investment'::text                                                            AS tx_type,
				TRIM(COALESCE(i.entity_name,''))                                              AS entity_name,
				%s                                                                            AS scheme_name,
				%s                                                                            AS amfi_scheme_code,
				COALESCE(i.amount,0)::numeric                                                 AS amount,
				COALESCE(c.status,'')                                                         AS status,
				COALESCE(la.processing_status,'')                                             AS processing_status,
				COALESCE(s.amc_name,'')                                                       AS amc_name,
				COALESCE(s.internal_scheme_code,'')                                           AS scheme_code,
				COALESCE(s.isin,'')                                                           AS isin,
				COALESCE(mf.folio_number,'')                                                  AS folio_number,
				COALESCE(md.demat_account_number,'')                                          AS demat_number,
				COALESCE(TO_CHAR(i.transaction_date,'YYYY-MM-DD'),'')                         AS transaction_date,
				''::text                                                                       AS transaction_type,
				COALESCE(c.allotted_units,0)::numeric                                         AS units,
				COALESCE(c.nav,0)::numeric                                                    AS nav,
				COALESCE(TO_CHAR(c.nav_date,'YYYY-MM-DD'),'')                                 AS nav_date,
				COALESCE(c.net_amount,0)::numeric                                             AS net_amount,
				COALESCE(c.stamp_duty,0)::numeric                                             AS stamp_duty,
				0::numeric                                                                     AS exit_load,
				0::numeric                                                                     AS tds,
				COALESCE(TO_CHAR(c.confirmed_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS confirmed_at,
				'Investment Suite'::text                                                       AS source,
				c.initiation_id::text                                                         AS initiation_id,
				c.confirmation_id::text                                                       AS confirmation_id,
				''::text                                                                       AS batch_id,
				COALESCE(s.scheme_id::text,'')                                                AS scheme_id,
				COALESCE(mf.folio_id::text,'')                                                AS folio_id,
				COALESCE(md.demat_id::text,'')                                                AS demat_id
			FROM investment.investment_confirmation c
			JOIN investment.investment_initiation i ON i.initiation_id = c.initiation_id
			%s %s %s
			LEFT JOIN LATERAL (
				SELECT processing_status FROM investment.auditactioninvestmentconfirmation
				WHERE confirmation_id = c.confirmation_id ORDER BY requested_at DESC LIMIT 1
			) la ON true
			WHERE COALESCE(c.is_deleted,false)=false%s`,
			schemeNameInvest, amfiInvest, schemeJoin, folioJoin, dematJoin,
			buildWhere("i.entity_name", constants.AMCName, constants.SchemeName, "c.status", "i.transaction_date", false))

		redemptSQL := fmt.Sprintf(`
			SELECT
				'Redemption'::text                                                            AS tx_type,
				TRIM(COALESCE(i.entity_name,''))                                              AS entity_name,
				%s                                                                            AS scheme_name,
				%s                                                                            AS amfi_scheme_code,
				COALESCE(c.gross_proceeds,0)::numeric                                         AS amount,
				COALESCE(c.status,'')                                                         AS status,
				COALESCE(la.processing_status,'')                                             AS processing_status,
				COALESCE(s.amc_name,'')                                                       AS amc_name,
				COALESCE(s.internal_scheme_code,'')                                           AS scheme_code,
				COALESCE(s.isin,'')                                                           AS isin,
				COALESCE(mf.folio_number,'')                                                  AS folio_number,
				COALESCE(md.demat_account_number,'')                                          AS demat_number,
				COALESCE(TO_CHAR(i.requested_date,'YYYY-MM-DD'),'')                           AS transaction_date,
				''::text                                                                       AS transaction_type,
				COALESCE(c.actual_units,0)::numeric                                           AS units,
				COALESCE(c.actual_nav,0)::numeric                                             AS nav,
				''::text                                                                       AS nav_date,
				COALESCE(c.net_credited,0)::numeric                                           AS net_amount,
				0::numeric                                                                     AS stamp_duty,
				COALESCE(c.exit_load,0)::numeric                                              AS exit_load,
				COALESCE(c.tds,0)::numeric                                                    AS tds,
				COALESCE(TO_CHAR(c.confirmed_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS confirmed_at,
				'Redemption Suite'::text                                                       AS source,
				c.redemption_id::text                                                         AS initiation_id,
				c.redemption_confirm_id::text                                                 AS confirmation_id,
				''::text                                                                       AS batch_id,
				COALESCE(s.scheme_id::text,'')                                                AS scheme_id,
				COALESCE(mf.folio_id::text,'')                                                AS folio_id,
				COALESCE(md.demat_id::text,'')                                                AS demat_id
			FROM investment.redemption_confirmation c
			JOIN investment.redemption_initiation i ON i.redemption_id = c.redemption_id
			%s %s %s
			LEFT JOIN LATERAL (
				SELECT processing_status FROM investment.auditactionredemptionconfirmation
				WHERE redemption_confirm_id = c.redemption_confirm_id ORDER BY requested_at DESC LIMIT 1
			) la ON true
			WHERE COALESCE(c.is_deleted,false)=false
			  AND NOT EXISTS (
			    SELECT 1
			    FROM investment.approved_onboard_transaction aot
			    WHERE LOWER(TRIM(COALESCE(aot.entity_name,''))) = LOWER(TRIM(COALESCE(i.entity_name,'')))
			      AND COALESCE(aot.transaction_date::date, aot.transaction_date) = COALESCE(i.requested_date::date, i.requested_date)
			      AND ABS(COALESCE(aot.amount,0) - COALESCE(c.gross_proceeds,0)) < 0.01
			      AND ABS(COALESCE(aot.units,0) - COALESCE(c.actual_units,0)) < 0.01
			      AND (
			        (NULLIF(TRIM(COALESCE(aot.scheme_id,'')), '') IS NOT NULL AND NULLIF(TRIM(COALESCE(i.scheme_id,'')), '') IS NOT NULL AND TRIM(aot.scheme_id) = TRIM(i.scheme_id))
			        OR (NULLIF(TRIM(COALESCE(aot.scheme_internal_code,'')), '') IS NOT NULL AND TRIM(aot.scheme_internal_code) = TRIM(COALESCE(s.internal_scheme_code, i.scheme_id, '')))
			      )
			  )%s`,
			schemeNameRedempt, amfiRedempt, schemeJoin, folioJoin, dematJoin,
			buildWhere("i.entity_name", constants.AMCName, constants.SchemeName, "c.status", "i.requested_date", false))

		onboardSchemeJoin := `LEFT JOIN LATERAL (
			SELECT ` + schemejoin.LateralSelectCols + `
			FROM investment.masterscheme
			WHERE ` + schemejoin.LateralWhereOnboardTx + `
			LIMIT 1
		) s ON true`
		onboardFolioJoin := `LEFT JOIN investment.masterfolio mf ON mf.folio_id = ot.folio_id`
		onboardDematJoin := `LEFT JOIN investment.masterdemataccount md ON md.demat_id = ot.demat_id`

		onboardSQL := fmt.Sprintf(`
			SELECT
				'Onboard'::text                                                               AS tx_type,
				TRIM(COALESCE(ot.entity_name, mf.entity_name, md.entity_name, ''))            AS entity_name,
				%s                                                                            AS scheme_name,
				%s                                                                            AS amfi_scheme_code,
				COALESCE(ot.amount,0)::numeric                                                AS amount,
				'ONBOARDED'::text                                                             AS status,
				COALESCE(ob.status, ot.approval_status, 'APPROVED')                          AS processing_status,
				COALESCE(s.amc_name,'')                                                       AS amc_name,
				COALESCE(s.internal_scheme_code,ot.scheme_internal_code,'')                   AS scheme_code,
				COALESCE(s.isin,'')                                                           AS isin,
				COALESCE(ot.folio_number,'')                                                  AS folio_number,
				COALESCE(ot.demat_acc_number,'')                                              AS demat_number,
				COALESCE(TO_CHAR(ot.transaction_date,'YYYY-MM-DD'),'')                        AS transaction_date,
				` + strings.Replace(SQLNormalizeTransactionType, "transaction_type", "ot.transaction_type", -1) + ` AS transaction_type,
				COALESCE(ot.units,0)::numeric                                                 AS units,
				COALESCE(ot.nav,0)::numeric                                                   AS nav,
				''::text                                                                       AS nav_date,
				COALESCE(ot.amount,0)::numeric                                                AS net_amount,
				0::numeric                                                                     AS stamp_duty,
				0::numeric                                                                     AS exit_load,
				0::numeric                                                                     AS tds,
				COALESCE(TO_CHAR(ob.completed_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS confirmed_at,
				COALESCE(NULLIF(TRIM(ob.source), ''), 'Workbench')                          AS source,
				''::text                                                                       AS initiation_id,
				''::text                                                                       AS confirmation_id,
				ot.batch_id::text                                                             AS batch_id,
				COALESCE(s.scheme_id::text,ot.scheme_id,'')                                   AS scheme_id,
				COALESCE(mf.folio_id::text,ot.folio_id,'')                                    AS folio_id,
				COALESCE(md.demat_id::text,ot.demat_id,'')                                    AS demat_id
			FROM investment.approved_onboard_transaction ot
			LEFT JOIN investment.onboard_batch ob ON ob.batch_id = ot.batch_id
			%s %s %s
			WHERE 1=1%s`,
			schemeNameOnboard, amfiOnboard, onboardSchemeJoin, onboardFolioJoin, onboardDematJoin,
			// skipStatus=true because onboard rows have fixed 'ONBOARDED' status
			buildWhere("COALESCE(ot.entity_name, mf.entity_name, md.entity_name)", constants.AMCName, constants.SchemeName, "ob.status", "ot.transaction_date", ps.statusIdx > 0))

		var parts []string
		txLower := strings.ToLower(f.TxType)
		if txLower == "" || txLower == "investment" {
			parts = append(parts, investSQL)
		}
		if txLower == "" || txLower == "redemption" {
			parts = append(parts, redemptSQL)
		}
		if txLower == "" || txLower == "onboard" {
			parts = append(parts, onboardSQL)
		}
		if len(parts) == 0 {
			api.RespondWithPayload(w, true, "", []PortfolioTxRow{})
			return
		}

		fullQuery := "SELECT * FROM (" + strings.Join(parts, " UNION ALL ") + ") t ORDER BY transaction_date DESC, entity_name"

		rows, err := pgxPool.Query(ctx, fullQuery, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		var results []PortfolioTxRow
		for rows.Next() {
			var row PortfolioTxRow
			if err := rows.Scan(
				&row.TxType, &row.EntityName, &row.SchemeName, &row.AmfiSchemeCode,
				&row.Amount, &row.Status, &row.ProcessingStatus,
				&row.AmcName, &row.SchemeCode, &row.ISIN, &row.FolioNumber, &row.DematNumber,
				&row.TransactionDate, &row.TransactionType, &row.Units, &row.Nav, &row.NavDate,
				&row.NetAmount, &row.StampDuty, &row.ExitLoad, &row.TDS,
				&row.ConfirmedAt, &row.Source,
				&row.InitiationID, &row.ConfirmationID, &row.BatchID,
				&row.SchemeID, &row.FolioID, &row.DematID,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "scan failed: "+err.Error())
				return
			}
			results = append(results, row)
		}
		for i := range results {
			results[i].EntityName = strings.TrimSpace(results[i].EntityName)
			results[i].TransactionType = NormalizeTransactionType(results[i].TransactionType)
		}
		if results == nil {
			results = []PortfolioTxRow{}
		}

		api.RespondWithPayload(w, true, "", results)
	}
}
