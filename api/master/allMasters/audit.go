package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Shared helpers ────────────────────────────────────────────────────────────

func scanAuditRows(rows pgx.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()
	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fields))
		for i, f := range fields {
			if vals[i] == nil {
				row[f.Name] = ""
			} else {
				row[f.Name] = vals[i]
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func respondAuditData(w http.ResponseWriter, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"success": true,
		"data":    payload,
	})
}

type auditHistoryConfig struct {
	AuditTable  string
	MasterTable string
	IDCol       string
	NameCol     string
	EntityID    string
}

// runAuditHistory executes the standard audit-history query pattern used by all cash masters.
// auditTable and masterTable are hardcoded constants — no user input.
func runAuditHistory(
	w http.ResponseWriter,
	r *http.Request,
	pgxPool *pgxpool.Pool,
	cfg auditHistoryConfig,
) {
	auditTable := cfg.AuditTable
	masterTable := cfg.MasterTable
	idCol := cfg.IDCol
	nameCol := cfg.NameCol
	entityID := cfg.EntityID
	if api.GetSessionFromCtx(r.Context()) == nil {
		api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
		return
	}
	ctx := r.Context()

	//nolint:gosec // table/column names are all hardcoded constants
	baseQ := `SELECT
		a.` + idCol + ` AS entity_id,
		a.actiontype,
		a.processing_status,
		COALESCE(a.reason, '') AS reason,
		COALESCE(a.requested_by, '') AS requested_by,
		a.requested_at,
		COALESCE(a.requested_ip, '') AS requested_ip,
		COALESCE(a.checker_by, '') AS checker_by,
		a.checker_at,
		COALESCE(a.checker_ip, '') AS checker_ip,
		COALESCE(a.checker_comment, '') AS checker_comment,
		COALESCE(m.` + nameCol + `, '') AS display_name
	FROM ` + auditTable + ` a
	LEFT JOIN ` + masterTable + ` m ON m.` + idCol + ` = a.` + idCol

	var q string
	var args []interface{}
	if strings.TrimSpace(entityID) != "" {
		q = baseQ + ` WHERE a.` + idCol + ` = $1 ORDER BY COALESCE(a.requested_at, '1970-01-01'::timestamp) DESC`
		args = append(args, entityID)
	} else {
		q = baseQ + ` ORDER BY COALESCE(a.requested_at, '1970-01-01'::timestamp) DESC LIMIT 200`
	}

	rows, err := pgxPool.Query(ctx, q, args...)
	if err != nil {
		api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
		return
	}
	payload, err := scanAuditRows(rows)
	if err != nil {
		api.RespondWithError(w, http.StatusInternalServerError, "failed to read audit history: "+err.Error())
		return
	}
	respondAuditData(w, payload)
}

// ─── Bank ──────────────────────────────────────────────────────────────────────
// POST /master/bank/audit-history   { bank_id? }

func GetBankMasterAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			BankID string `json:"bank_id"`
		}
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		if api.GetSessionFromCtx(r.Context()) == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		// Bank master keeps its previous field values in old_* columns on masterbank
		// (populated on EDIT). Return both current and old_* values so the audit
		// timeline can render a field-level change summary.
		baseQ := `SELECT
			a.bank_id AS entity_id,
			a.actiontype,
			a.processing_status,
			COALESCE(a.reason, '') AS reason,
			COALESCE(a.requested_by, '') AS requested_by,
			a.requested_at,
			COALESCE(a.requested_ip, '') AS requested_ip,
			COALESCE(a.checker_by, '') AS checker_by,
			a.checker_at,
			COALESCE(a.checker_ip, '') AS checker_ip,
			COALESCE(a.checker_comment, '') AS checker_comment,
			COALESCE(m.bank_name, '') AS display_name,
			COALESCE(m.bank_name, '') AS bank_name,
			COALESCE(m.old_bank_name, '') AS old_bank_name,
			COALESCE(m.bank_short_name, '') AS bank_short_name,
			COALESCE(m.old_bank_short_name, '') AS old_bank_short_name,
			COALESCE(m.swift_bic_code, '') AS swift_bic_code,
			COALESCE(m.old_swift_bic_code, '') AS old_swift_bic_code,
			COALESCE(m.country_of_headquarters, '') AS country_of_headquarters,
			COALESCE(m.old_country_of_headquarters, '') AS old_country_of_headquarters,
			COALESCE(m.connectivity_type, '') AS connectivity_type,
			COALESCE(m.old_connectivity_type, '') AS old_connectivity_type,
			COALESCE(m.active_status, '') AS active_status,
			COALESCE(m.old_active_status, '') AS old_active_status,
			COALESCE(m.contact_person_name, '') AS contact_person_name,
			COALESCE(m.old_contact_person_name, '') AS old_contact_person_name,
			COALESCE(m.contact_person_email, '') AS contact_person_email,
			COALESCE(m.old_contact_person_email, '') AS old_contact_person_email,
			COALESCE(m.contact_person_phone, '') AS contact_person_phone,
			COALESCE(m.old_contact_person_phone, '') AS old_contact_person_phone,
			COALESCE(m.address_line1, '') AS address_line1,
			COALESCE(m.old_address_line1, '') AS old_address_line1,
			COALESCE(m.address_line2, '') AS address_line2,
			COALESCE(m.old_address_line2, '') AS old_address_line2,
			COALESCE(m.city, '') AS city,
			COALESCE(m.old_city, '') AS old_city,
			COALESCE(m.state_province, '') AS state_province,
			COALESCE(m.old_state_province, '') AS old_state_province,
			COALESCE(m.postal_code, '') AS postal_code,
			COALESCE(m.old_postal_code, '') AS old_postal_code
		FROM auditactionbank a
		LEFT JOIN masterbank m ON m.bank_id = a.bank_id`

		var q string
		var args []interface{}
		if strings.TrimSpace(req.BankID) != "" {
			q = baseQ + ` WHERE a.bank_id = $1 ORDER BY COALESCE(a.requested_at, '1970-01-01'::timestamp) DESC`
			args = append(args, req.BankID)
		} else {
			q = baseQ + ` ORDER BY COALESCE(a.requested_at, '1970-01-01'::timestamp) DESC LIMIT 200`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := scanAuditRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read audit history: "+err.Error())
			return
		}
		respondAuditData(w, payload)
	}
}

// ─── Currency ─────────────────────────────────────────────────────────────────
// POST /master/currency/audit-history   { currency_id? }

func GetCurrencyMasterAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CurrencyID string `json:"currency_id"`
		}
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		runAuditHistory(w, r, pgxPool, auditHistoryConfig{AuditTable: "auditactioncurrency", MasterTable: "mastercurrency", IDCol: "currency_id", NameCol: "currency_name", EntityID: req.CurrencyID})
	}
}

// ─── GL Account ───────────────────────────────────────────────────────────────
// POST /master/glaccount/audit-history   { gl_account_id? }

func GetGLAccountMasterAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			GLAccountID string `json:"gl_account_id"`
		}
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		runAuditHistory(w, r, pgxPool, auditHistoryConfig{AuditTable: "auditactionglaccount", MasterTable: "masterglaccount", IDCol: "gl_account_id", NameCol: "gl_account_name", EntityID: req.GLAccountID})
	}
}

// ─── Cash Flow Category ───────────────────────────────────────────────────────
// POST /master/cashflow-category/audit-history   { category_id? }

func GetCashFlowCategoryAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CategoryID string `json:"category_id"`
		}
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		runAuditHistory(w, r, pgxPool, auditHistoryConfig{AuditTable: "auditactioncashflowcategory", MasterTable: "mastercashflowcategory", IDCol: "category_id", NameCol: "category_name", EntityID: req.CategoryID})
	}
}

// ─── Cost Profit Center ───────────────────────────────────────────────────────
// POST /master/costprofit-center/audit-history   { centre_id? }

func GetCostProfitCenterAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CentreID string `json:"centre_id"`
		}
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		runAuditHistory(w, r, pgxPool, auditHistoryConfig{AuditTable: "auditactioncostprofitcenter", MasterTable: "mastercostprofitcenter", IDCol: "centre_id", NameCol: "centre_name", EntityID: req.CentreID})
	}
}

// ─── Counterparty ─────────────────────────────────────────────────────────────
// POST /master/counterparty/audit-history   { counterparty_id? }

func GetCounterpartyAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CounterpartyID string `json:"counterparty_id"`
		}
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		runAuditHistory(w, r, pgxPool, auditHistoryConfig{AuditTable: "auditactioncounterparty", MasterTable: "mastercounterparty", IDCol: "counterparty_id", NameCol: "counterparty_name", EntityID: req.CounterpartyID})
	}
}

// ─── Bank Account ─────────────────────────────────────────────────────────────
// POST /master/bankaccount/audit-history   { account_id? }

func GetBankAccountAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			AccountID string `json:"account_id"`
		}
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		runAuditHistory(w, r, pgxPool, auditHistoryConfig{AuditTable: "auditactionbankaccount", MasterTable: "masterbankaccount", IDCol: "account_id", NameCol: "account_number", EntityID: req.AccountID})
	}
}

// ─── Payable / Receivable ─────────────────────────────────────────────────────
// POST /master/payablereceivable/audit-history   { type_id? }

func GetPayableReceivableAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TypeID string `json:"type_id"`
		}
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		runAuditHistory(w, r, pgxPool, auditHistoryConfig{AuditTable: "auditactionpayablereceivable", MasterTable: "masterpayablereceivabletype", IDCol: "type_id", NameCol: "type_name", EntityID: req.TypeID})
	}
}

// ─── Entity Cash ──────────────────────────────────────────────────────────────
// POST /master/entitycash/audit-history   { entity_id? }

func GetEntityCashAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntityID string `json:"entity_id"`
		}
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		runAuditHistory(w, r, pgxPool, auditHistoryConfig{AuditTable: "auditactionentity", MasterTable: "masterentitycash", IDCol: "entity_id", NameCol: "entity_name", EntityID: req.EntityID})
	}
}
