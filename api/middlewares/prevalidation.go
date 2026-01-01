package api

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/validation"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"

	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func PreValidationMiddleware(db *pgxpool.Pool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			body, err := io.ReadAll(r.Body)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to read request body")
				return
			}
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewBuffer(body))

			userID, err := validation.ExtractUserID(r)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
				return
			}

			r.Body = io.NopCloser(bytes.NewBuffer(body))

			session := validation.ValidateSession(userID)
			if session == nil {
				api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
				return
			}

			validationResult, err := validation.PreValidateRequest(ctx, db, userID)
			if err != nil {
				le := strings.ToLower(err.Error())
				if err == http.ErrMissingFile || strings.Contains(le, "no business") || strings.Contains(le, "no entity") || strings.Contains(le, "no accessible") {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{
						constants.ValueSuccess: false,
						"error":                "No accessible business units found for this user",
						"code":                 "NO_ACCESS_ENTITIES",
						"help":                 "Contact your administrator to grant access to business units or set up entities for your account.",
					})
					return
				}
				api.RespondWithError(w, http.StatusUnauthorized, "Validation failed: "+err.Error())
				return
			}

			entityIDs, entityNames, err := resolveEntityHierarchy(ctx, db, validationResult.RootEntityID)
			if err != nil {
				if err == http.ErrMissingFile {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": "No accessible business units found"})
					return
				}
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to resolve entity hierarchy: "+err.Error())
				return
			}

			adminOverrideApplied := false

			// If admin override is enabled and the user is whitelisted or their role is whitelisted, preload everything
			if IsAdminOverrideEnabled() && IsAdminUser(userID) {
				// user-id based override (highest precedence)
				allEntityIDs, allEntityNames, entErr := LoadAllEntities(ctx, db)
				if entErr == nil && len(allEntityIDs) > 0 {
					entityIDs = allEntityIDs
					entityNames = allEntityNames
				}

				all, errs := LoadEverythingIntoContext(ctx, db)
				for k, v := range all {
					ctx = context.WithValue(ctx, k, v)
				}
				if len(errs) > 0 {
					ctx = context.WithValue(ctx, "admin_override_load_errors", errs)
				}
				ctx = context.WithValue(ctx, "is_admin_override", true)
				ctx = context.WithValue(ctx, "admin_override_by", "user")
				adminOverrideApplied = true
			} else if IsAdminOverrideEnabled() {
				// first try in-memory session role (preferred)
				roleMatched := false
				matchedRoles := []string{}
				if session.Role != "" && IsRoleAdminName(session.Role) {
					roleMatched = true
					matchedRoles = append(matchedRoles, session.Role)
				}
				if !roleMatched && session.RoleCode != "" && IsRoleAdminName(session.RoleCode) {
					roleMatched = true
					matchedRoles = append(matchedRoles, session.RoleCode)
				}
				// fallback to DB role lookup only if session had no role info
				if !roleMatched {
					isRoleAdmin, dbMatched, roleErr := IsUserInAdminRole(ctx, db, userID)
					if roleErr != nil {
						ctx = context.WithValue(ctx, "admin_override_load_errors", []string{"role_lookup: " + roleErr.Error()})
					}
					if isRoleAdmin {
						roleMatched = true
						matchedRoles = append(matchedRoles, dbMatched...)
					}
				}

				if roleMatched {
					// role-based override
					allEntityIDs, allEntityNames, entErr := LoadAllEntities(ctx, db)
					if entErr == nil && len(allEntityIDs) > 0 {
						entityIDs = allEntityIDs
						entityNames = allEntityNames
					}
					all, errs := LoadEverythingIntoContext(ctx, db)
					for k, v := range all {
						ctx = context.WithValue(ctx, k, v)
					}
					// attach matched role info and audit
					ctx = context.WithValue(ctx, "is_admin_override", true)
					ctx = context.WithValue(ctx, "admin_override_by", "role")
					ctx = context.WithValue(ctx, "admin_override_role", matchedRoles)
					if len(errs) > 0 {
						ctx = context.WithValue(ctx, "admin_override_load_errors", errs)
					}
					// audit log
					log.Printf("[AUDIT] AdminOverride applied for user=%s by=role matched=%v", userID, matchedRoles)
					adminOverrideApplied = true
				}
			}

			// IMPORTANT: if admin override is enabled but not applied for this user,
			// we must still load the standard approval context; otherwise bank/currency/account
			// validations will fail for every request.
			if !adminOverrideApplied {
				banks, _ := loadApprovedBanks(ctx, db)
				currencies, _ := loadApprovedCurrencies(ctx, db)
				cashFlowCategories, _ := loadApprovedCashFlowCategories(ctx, db)
				amcs, _ := loadApprovedAMCs(ctx, db)
				schemes, _ := loadApprovedSchemes(ctx, db)
				dps, _ := loadApprovedDPs(ctx, db)
				bankAccounts, _ := loadApprovedBankAccounts(ctx, db)
				folios, _ := loadApprovedFolios(ctx, db)
				demats, _ := loadApprovedDemats(ctx, db)

				log.Printf("\n========== PREVALIDATION DEBUG ==========\n")
				log.Printf("User ID: %s\n", userID)
				log.Printf("Root Entity: %s (%s)\n", validationResult.RootEntityName, validationResult.RootEntityID)
				log.Printf("Business Unit: %s\n", validationResult.BusinessUnit)
				log.Printf("\nEntity Hierarchy (%d entities):\n", len(entityNames))
				for i, name := range entityNames {
					log.Printf("  [%d] %s (ID: %s)\n", i+1, name, entityIDs[i])
				}
				log.Printf("\nApproved AMCs (%d):\n", len(amcs))
				for i, amc := range amcs {
					if i < 5 || i >= len(amcs)-2 {
						log.Printf("  [%d] %s (ID: %s, Code: %s)\n", i+1, amc["amc_name"], amc["amc_id"], amc["internal_amc_code"])
					} else if i == 5 {
						log.Printf("  ... (%d more) ...\n", len(amcs)-7)
					}
				}
				log.Printf("\nApproved Schemes (%d):\n", len(schemes))
				if len(schemes) > 0 {
					log.Printf("  [1] %s (ID: %s)\n", schemes[0]["scheme_name"], schemes[0]["scheme_id"])
					if len(schemes) > 1 {
						log.Printf("  ... (%d more) ...\n", len(schemes)-1)
					}
				}
				log.Printf("\nApproved DPs (%d):\n", len(dps))
				for i, dp := range dps {
					log.Printf("  [%d] %s (ID: %s)\n", i+1, dp["dp_name"], dp["dp_id"])
				}
				log.Printf("\nApproved Bank Accounts (%d):\n", len(bankAccounts))
				for i, acc := range bankAccounts {
					log.Printf("  [%d] Account ID: %s | Account Number: %s | Nickname: %s | Bank: %s | Entity: %s\n",
						i+1, acc["account_id"], acc["account_number"], acc["account_name"], acc["bank_name"], acc["entity_name"])
				}
				log.Printf("\nApproved Folios (%d):\n", len(folios))
				for i, f := range folios {
					if i < 5 || i >= len(folios)-2 {
						log.Printf("  [%d] %s (ID: %s, AMC: %s)\n", i+1, f["folio_number"], f["folio_id"], f["amc_name"])
					} else if i == 5 {
						log.Printf("  ... (%d more) ...\n", len(folios)-7)
					}
				}
				log.Printf("\nApproved Demats (%d):\n", len(demats))
				for i, d := range demats {
					log.Printf("  [%d] %s (ID: %s, DP: %s)\n", i+1, d["demat_account_number"], d["demat_id"], d["dp_id"])
					if i >= 20 {
						break
					}
				}
				log.Printf("=========================================\n\n")

				ctx = context.WithValue(ctx, "BankInfo", banks)
				ctx = context.WithValue(ctx, "ActiveCurrencies", currencies)
				ctx = context.WithValue(ctx, "CashFlowCategories", cashFlowCategories)
				ctx = context.WithValue(ctx, "ApprovedAMCs", amcs)
				ctx = context.WithValue(ctx, "ApprovedSchemes", schemes)
				ctx = context.WithValue(ctx, "ApprovedDPs", dps)
				ctx = context.WithValue(ctx, "ApprovedBankAccounts", bankAccounts)
				ctx = context.WithValue(ctx, "ApprovedFolios", folios)
				ctx = context.WithValue(ctx, "ApprovedDemats", demats)
			}
			// log.Printf("\n========== PREVALIDATION DEBUG ==========\n")
			log.Printf("User ID: %s\n", userID)
			log.Printf("Root Entity: %s (%s)\n", validationResult.RootEntityName, validationResult.RootEntityID)
			log.Printf("Business Unit: %s\n", validationResult.BusinessUnit)
			log.Printf("\nEntity Hierarchy (%d entities):\n", len(entityNames))
			for i, name := range entityNames {
				log.Printf("  [%d] %s (ID: %s)\n", i+1, name, entityIDs[i])
			}
			// log.Printf("\nApproved AMCs (%d):\n", len(amcs))
			// for i, amc := range amcs {
			// 	if i < 5 || i >= len(amcs)-2 {
			// 		log.Printf("  [%d] %s (ID: %s, Code: %s)\n", i+1, amc["amc_name"], amc["amc_id"], amc["internal_amc_code"])
			// 	} else if i == 5 {
			// 		log.Printf("  ... (%d more) ...\n", len(amcs)-7)
			// 	}
			// }
			// log.Printf("\nApproved Schemes (%d):\n", len(schemes))
			// if len(schemes) > 0 {
			// 	log.Printf("  [1] %s (ID: %s)\n", schemes[0]["scheme_name"], schemes[0]["scheme_id"])
			// 	if len(schemes) > 1 {
			// 		log.Printf("  ... (%d more) ...\n", len(schemes)-1)
			// 	}
			// }
			// log.Printf("\nApproved DPs (%d):\n", len(dps))
			// for i, dp := range dps {
			// 	log.Printf("  [%d] %s (ID: %s)\n", i+1, dp["dp_name"], dp["dp_id"])
			// }
			// log.Printf("\nApproved Bank Accounts (%d):\n", len(bankAccounts))
			// for i, acc := range bankAccounts {
			// 	log.Printf("  [%d] Account ID: %s | Account Number: %s | Nickname: %s | Bank: %s | Entity: %s\n",
			// 		i+1, acc["account_id"], acc["account_number"], acc["account_name"], acc["bank_name"], acc["entity_name"])
			// }
			// log.Printf("\nApproved Folios (%d):\n", len(folios))
			// for i, f := range folios {
			// 	if i < 5 || i >= len(folios)-2 {
			// 		log.Printf("  [%d] %s (ID: %s, AMC: %s)\n", i+1, f["folio_number"], f["folio_id"], f["amc_name"])
			// 	} else if i == 5 {
			// 		log.Printf("  ... (%d more) ...\n", len(folios)-7)
			// 	}
			// }
			// log.Printf("\nApproved Demats (%d):\n", len(demats))
			// for i, d := range demats {
			// 	log.Printf("  [%d] %s (ID: %s, DP: %s)\n", i+1, d["demat_account_number"], d["demat_id"], d["dp_id"])
			// 	if i >= 20 {
			// 		break
			// 	}
			// }
			// log.Printf("=========================================\n\n")

			ctx = context.WithValue(ctx, "user_id", userID)
			ctx = context.WithValue(ctx, "session", session)
			ctx = context.WithValue(ctx, "business_unit", validationResult.BusinessUnit)
			ctx = context.WithValue(ctx, "root_entity_id", validationResult.RootEntityID)
			ctx = context.WithValue(ctx, "root_entity_name", validationResult.RootEntityName)
			ctx = context.WithValue(ctx, api.BusinessUnitsKey, entityNames)
			ctx = context.WithValue(ctx, api.EntityIDsKey, entityIDs)
			r.Body = io.NopCloser(bytes.NewBuffer(body))

			// Additional context debug dump (always log so we can inspect admin override and normal flows)
			entityIDsDump := api.GetEntityIDsFromCtx(ctx)
			bankNamesDump := api.GetBankNamesFromCtx(ctx)
			currCodesDump := api.GetCurrencyCodesFromCtx(ctx)
			// approved accounts stored as []map[string]string under "ApprovedBankAccounts"
			acctNums := make([]string, 0)
			if v := ctx.Value("ApprovedBankAccounts"); v != nil {
				if bankAccounts, ok := v.([]map[string]string); ok {
					for _, a := range bankAccounts {
						if s, has := a["account_number"]; has && strings.TrimSpace(s) != "" {
							acctNums = append(acctNums, strings.TrimSpace(s))
						}
					}
				}
			}
			log.Printf("[PREVALIDATION CONTEXT] user=%s entities=%v banks=%v accounts_count=%d currencies=%v is_admin_override=%v admin_override_by=%v\n",
				userID,
				entityIDsDump,
				bankNamesDump,
				len(acctNums),
				currCodesDump,
				ctx.Value("is_admin_override"),
				ctx.Value("admin_override_by"),
			)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
func GetUserIDFromContext(ctx context.Context) string {
	if userID, ok := ctx.Value("user_id").(string); ok {
		return userID
	}
	return ""
}

func GetSessionFromContext(ctx context.Context) *auth.UserSession {
	if session, ok := ctx.Value("session").(*auth.UserSession); ok {
		return session
	}
	return nil
}

func GetBusinessUnitFromContext(ctx context.Context) string {
	if bu, ok := ctx.Value("business_unit").(string); ok {
		return bu
	}
	return ""
}

func GetRootEntityFromContext(ctx context.Context) (id string, name string) {
	if entityID, ok := ctx.Value("root_entity_id").(string); ok {
		id = entityID
	}
	if entityName, ok := ctx.Value("root_entity_name").(string); ok {
		name = entityName
	}
	return id, name
}

func resolveEntityHierarchy(ctx context.Context, db *pgxpool.Pool, rootEntityId string) ([]string, []string, error) {
	var buEntityIDs []string
	var buNames []string
	entityMap := make(map[string]bool)

	query1 := `
	WITH RECURSIVE descendants AS (
		SELECT entity_id, entity_name
		FROM masterentitycash
		WHERE entity_id = $1
		UNION ALL
		SELECT me.entity_id, me.entity_name
		FROM masterentitycash me
		INNER JOIN cashentityrelationships er ON me.entity_name = er.child_entity_name
		INNER JOIN descendants d ON er.parent_entity_name = d.entity_name
	)
	SELECT DISTINCT entity_id, entity_name FROM descendants
	`

	rows1, err1 := db.Query(ctx, query1, rootEntityId)
	if err1 == nil {
		defer rows1.Close()
		for rows1.Next() {
			var entityID, entityName string
			if err := rows1.Scan(&entityID, &entityName); err == nil {
				if !entityMap[entityID] {
					buEntityIDs = append(buEntityIDs, entityID)
					buNames = append(buNames, entityName)
					entityMap[entityID] = true
				}
			}
		}
	}

	query2 := `
	WITH RECURSIVE descendants AS (
		SELECT entity_id, entity_name
		FROM masterEntity
		WHERE entity_id = $1
		UNION ALL
		SELECT me.entity_id, me.entity_name
		FROM masterEntity me
		INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
		INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
	)
	SELECT DISTINCT entity_id, entity_name FROM descendants
	`

	rows2, err2 := db.Query(ctx, query2, rootEntityId)
	if err2 == nil {
		defer rows2.Close()
		for rows2.Next() {
			var entityID, entityName string
			if err := rows2.Scan(&entityID, &entityName); err == nil {
				if !entityMap[entityID] {
					buEntityIDs = append(buEntityIDs, entityID)
					buNames = append(buNames, entityName)
					entityMap[entityID] = true
				}
			}
		}
	}

	if len(buNames) == 0 {
		return nil, nil, http.ErrMissingFile
	}

	return buEntityIDs, buNames, nil
}

func loadApprovedBanks(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (bank_id) 
				bank_id, 
				processing_status
			FROM auditactionbank
			WHERE processing_status = 'APPROVED'
			ORDER BY bank_id, requested_at DESC
		)
		SELECT 
			m.bank_id,
			m.bank_name,
			COALESCE(m.bank_short_name, '') as bank_short_name
		FROM masterbank m
		JOIN latest_approved l ON l.bank_id = m.bank_id
		WHERE LOWER(m.active_status) = 'active'
		  AND COALESCE(m.is_deleted, false) = false
		ORDER BY m.bank_name
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	banks := make([]map[string]string, 0)
	for rows.Next() {
		var bankID, bankName, bankShort string
		if err := rows.Scan(&bankID, &bankName, &bankShort); err == nil {
			banks = append(banks, map[string]string{
				"bank_id":         bankID,
				"bank_name":       bankName,
				"bank_short_name": bankShort,
			})
		}
	}

	return banks, nil
}

func loadApprovedCurrencies(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (currency_id) 
				currency_id, 
				processing_status
			FROM auditactioncurrency
			WHERE processing_status = 'APPROVED'
			ORDER BY currency_id, requested_at DESC
		)
		SELECT 
			m.currency_id,
			m.currency_code,
			m.currency_name
		FROM mastercurrency m
		JOIN latest_approved l ON l.currency_id = m.currency_id
		WHERE LOWER(m.status) = 'active'
		ORDER BY m.currency_code
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	currencies := make([]map[string]string, 0)
	for rows.Next() {
		var currencyID, currencyCode, currencyName string
		if err := rows.Scan(&currencyID, &currencyCode, &currencyName); err == nil {
			currencies = append(currencies, map[string]string{
				"currency_id":   currencyID,
				"currency_code": currencyCode,
				"currency_name": currencyName,
			})
		}
	}

	return currencies, nil
}

func loadApprovedCashFlowCategories(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (category_id) 
				category_id, 
				processing_status
			FROM auditactioncashflowcategory
			WHERE processing_status = 'APPROVED'
			ORDER BY category_id, requested_at DESC
		)
		SELECT 
			m.category_id,
			m.category_name,
			m.category_type
		FROM mastercashflowcategory m
		JOIN latest_approved l ON l.category_id = m.category_id
		WHERE UPPER(m.status) = 'ACTIVE'
		  AND COALESCE(m.is_deleted, false) = false
		ORDER BY m.category_name
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	categories := make([]map[string]string, 0)
	for rows.Next() {
		var categoryID, categoryName, categoryType string
		if err := rows.Scan(&categoryID, &categoryName, &categoryType); err == nil {
			categories = append(categories, map[string]string{
				"category_id":   categoryID,
				"category_name": categoryName,
				"category_type": categoryType,
			})
		}
	}

	return categories, nil
}

func loadApprovedAMCs(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (amc_id) 
				amc_id, 
				processing_status
			FROM investment.auditactionamc
			WHERE processing_status = 'APPROVED'
			ORDER BY amc_id, requested_at DESC
		)
		SELECT 
			m.amc_id,
			m.amc_name,
			m.internal_amc_code
		FROM investment.masteramc m
		JOIN latest_approved l ON l.amc_id = m.amc_id
		WHERE UPPER(m.status) = 'ACTIVE'
		  AND COALESCE(m.is_deleted, false) = false
		ORDER BY m.amc_name
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	amcs := make([]map[string]string, 0)
	for rows.Next() {
		var amcID, amcName, amcCode string
		if err := rows.Scan(&amcID, &amcName, &amcCode); err == nil {
			amcs = append(amcs, map[string]string{
				"amc_id":            amcID,
				"amc_name":          amcName,
				"internal_amc_code": amcCode,
			})
		}
	}

	return amcs, nil
}

func loadApprovedSchemes(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (scheme_id) 
				scheme_id, 
				processing_status
			FROM investment.auditactionscheme
			WHERE processing_status = 'APPROVED'
			ORDER BY scheme_id, requested_at DESC
		)
		SELECT 
			m.scheme_id,
			m.scheme_name,
			m.isin,
			m.internal_scheme_code,
			m.amc_name
		FROM investment.masterscheme m
		JOIN latest_approved l ON l.scheme_id = m.scheme_id
		WHERE UPPER(m.status) = 'ACTIVE'
		  AND COALESCE(m.is_deleted, false) = false
		ORDER BY m.scheme_name
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	schemes := make([]map[string]string, 0)
	for rows.Next() {
		var schemeID, schemeName, isin, schemeCode, amcName string
		if err := rows.Scan(&schemeID, &schemeName, &isin, &schemeCode, &amcName); err == nil {
			schemes = append(schemes, map[string]string{
				"scheme_id":            schemeID,
				"scheme_name":          schemeName,
				"isin":                 isin,
				"internal_scheme_code": schemeCode,
				"amc_name":             amcName,
			})
		}
	}

	return schemes, nil
}

func loadApprovedDPs(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (dp_id) 
				dp_id, 
				processing_status
			FROM investment.auditactiondp
			WHERE processing_status = 'APPROVED'
			ORDER BY dp_id, requested_at DESC
		)
		SELECT 
			m.dp_id,
			m.dp_name,
			m.dp_code,
			m.depository
		FROM investment.masterdepositoryparticipant m
		JOIN latest_approved l ON l.dp_id = m.dp_id
		WHERE UPPER(m.status) = 'ACTIVE'
		  AND COALESCE(m.is_deleted, false) = false
		ORDER BY m.dp_name
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	dps := make([]map[string]string, 0)
	for rows.Next() {
		var dpID, dpName, dpCode, depository string
		if err := rows.Scan(&dpID, &dpName, &dpCode, &depository); err == nil {
			dps = append(dps, map[string]string{
				"dp_id":      dpID,
				"dp_name":    dpName,
				"dp_code":    dpCode,
				"depository": depository,
			})
		}
	}

	return dps, nil
}

func loadApprovedBankAccounts(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (account_id) 
				account_id, 
				processing_status
			FROM public.auditactionbankaccount
			WHERE processing_status = 'APPROVED'
			ORDER BY account_id, requested_at DESC
		)
		SELECT 
			m.account_id,
			m.account_number,
			COALESCE(m.account_nickname, '') AS account_name,
			COALESCE(b.bank_name, '') AS bank_name,
			COALESCE(e.entity_name, ec.entity_name, '') AS entity_name
		FROM public.masterbankaccount m
		JOIN latest_approved l ON l.account_id = m.account_id
		LEFT JOIN public.masterbank b ON b.bank_id = m.bank_id
		LEFT JOIN public.masterentity e ON e.entity_id::text = m.entity_id
		LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = m.entity_id
		WHERE UPPER(m.status) = 'ACTIVE'
		  AND COALESCE(m.is_deleted, false) = false
		ORDER BY m.account_number
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	bankAccounts := make([]map[string]string, 0)
	for rows.Next() {
		var accountID, accountNumber, accountName, bankName, entityName string
		if err := rows.Scan(&accountID, &accountNumber, &accountName, &bankName, &entityName); err == nil {
			bankAccounts = append(bankAccounts, map[string]string{
				"account_id":     accountID,
				"account_number": accountNumber,
				"account_name":   accountName,
				"bank_name":      bankName,
				"entity_name":    entityName,
			})
		}
	}

	return bankAccounts, nil
}

func loadApprovedFolios(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (folio_id)
				folio_id,
				processing_status
			FROM investment.auditactionfolio
			WHERE processing_status = 'APPROVED'
			ORDER BY folio_id, requested_at DESC
		)
		SELECT
			m.folio_id,
			m.folio_number,
			COALESCE(m.amc_name,'') AS amc_name,
			COALESCE(m.scheme_id,'') AS scheme_id,
			COALESCE(m.entity_name,'') AS entity_name
		FROM investment.masterfolio m
		JOIN latest_approved l ON l.folio_id = m.folio_id
		WHERE UPPER(m.status) = 'ACTIVE'
		  AND COALESCE(m.is_deleted, false) = false
		ORDER BY m.folio_number
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	folios := make([]map[string]string, 0)
	for rows.Next() {
		var folioID, folioNumber, amcName, schemeID, entityName string
		if err := rows.Scan(&folioID, &folioNumber, &amcName, &schemeID, &entityName); err == nil {
			folios = append(folios, map[string]string{
				"folio_id":     folioID,
				"folio_number": folioNumber,
				"amc_name":     amcName,
				"scheme_id":    schemeID,
				"entity_name":  entityName,
			})
		}
	}

	return folios, nil
}

func loadApprovedDemats(ctx context.Context, db *pgxpool.Pool) ([]map[string]string, error) {
	query := `
		WITH latest_approved AS (
			SELECT DISTINCT ON (demat_id)
				demat_id,
				processing_status
			FROM investment.auditactiondemat
			WHERE processing_status = 'APPROVED'
			ORDER BY demat_id, requested_at DESC
		)
		SELECT
			m.demat_id,
			COALESCE(m.dp_id,'') AS dp_id,
			COALESCE(m.depository_participant,'') AS depository_participant,
			COALESCE(m.demat_account_number,'') AS demat_account_number,
			COALESCE(m.entity_name,'') AS entity_name,
			COALESCE(m.default_settlement_account,'') AS default_settlement_account
		FROM investment.masterdemataccount m
		JOIN latest_approved l ON l.demat_id = m.demat_id
		WHERE UPPER(m.status) = 'ACTIVE'
		  AND COALESCE(m.is_deleted, false) = false
		ORDER BY m.demat_account_number
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return []map[string]string{}, err
	}
	defer rows.Close()

	demats := make([]map[string]string, 0)
	for rows.Next() {
		var dematID, dpID, dpParticipant, dematNumber, entityName, defaultAccount string
		if err := rows.Scan(&dematID, &dpID, &dpParticipant, &dematNumber, &entityName, &defaultAccount); err == nil {
			demats = append(demats, map[string]string{
				"demat_id":                   dematID,
				"dp_id":                      dpID,
				"depository_participant":     dpParticipant,
				"demat_account_number":       dematNumber,
				"entity_name":                entityName,
				"default_settlement_account": defaultAccount,
			})
		}
	}

	return demats, nil
}
