package api

import (
    "context"
    "log"
    "os"
    "strings"
    "sync"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/joho/godotenv"
)

var (
    adminUserIDs []string
    adminOnce    sync.Once
    adminRoleNames []string
    adminRoleOnce  sync.Once
)

func init() {
    // Try to load .env if present (optional)
    _ = godotenv.Load()
}

// loadAdminList populates adminUserIDs from env variable ADMIN_USER_IDS
// Format: comma separated user IDs, e.g. "user1,user2,user3"
func loadAdminList() {
    adminOnce.Do(func() {
        raw := os.Getenv("ADMIN_USER_IDS")
        if raw == "" {
            adminUserIDs = []string{}
            return
        }
        parts := strings.Split(raw, ",")
        out := make([]string, 0, len(parts))
        for _, p := range parts {
            t := strings.TrimSpace(p)
            if t != "" {
                out = append(out, t)
            }
        }
        adminUserIDs = out
    })
}

// loadAdminRoles populates adminRoleNames from env variable ADMIN_ROLES
// Format: comma separated role names or role codes, e.g. "superadmin,platform_admin"
func loadAdminRoles() {
    adminRoleOnce.Do(func() {
        raw := os.Getenv("ADMIN_ROLES")
        if raw == "" {
            adminRoleNames = []string{}
            return
        }
        parts := strings.Split(raw, ",")
        out := make([]string, 0, len(parts))
        for _, p := range parts {
            t := strings.ToLower(strings.TrimSpace(p))
            if t != "" {
                out = append(out, t)
            }
        }
        adminRoleNames = out
    })
}

// IsRoleAdminName checks whether a role name or code matches the admin roles list
func IsRoleAdminName(roleNameOrCode string) bool {
    if roleNameOrCode == "" {
        return false
    }
    loadAdminRoles()
    rn := strings.ToLower(strings.TrimSpace(roleNameOrCode))
    for _, v := range adminRoleNames {
        if v == rn {
            return true
        }
    }
    return false
}

// GetUserRoles returns role names and role codes for a given user id by querying the DB
func GetUserRoles(ctx context.Context, db *pgxpool.Pool, userID string) ([]string, []string, error) {
    names := []string{}
    codes := []string{}
    if userID == "" {
        return names, codes, nil
    }
    query := `SELECT r.name, COALESCE(r.rolecode, '') FROM roles r JOIN user_roles ur ON ur.role_id = r.id WHERE ur.user_id = $1`
    rows, err := db.Query(ctx, query, userID)
    if err != nil {
        return names, codes, err
    }
    defer rows.Close()
    for rows.Next() {
        var name, code string
        if err := rows.Scan(&name, &code); err == nil {
            names = append(names, name)
            codes = append(codes, code)
        }
    }
    return names, codes, nil
}

// IsUserInAdminRole checks whether the user has any role that matches ADMIN_ROLES.
// Returns matched role names/codes and any DB error encountered.
func IsUserInAdminRole(ctx context.Context, db *pgxpool.Pool, userID string) (bool, []string, error) {
    loadAdminRoles()
    if len(adminRoleNames) == 0 {
        return false, nil, nil
    }
    names, codes, err := GetUserRoles(ctx, db, userID)
    if err != nil {
        return false, nil, err
    }
    matched := []string{}
    for _, n := range names {
        if IsRoleAdminName(n) {
            matched = append(matched, n)
        }
    }
    for _, c := range codes {
        if IsRoleAdminName(c) {
            // avoid duplicate
            already := false
            for _, m := range matched {
                if strings.EqualFold(m, c) {
                    already = true
                    break
                }
            }
            if !already {
                matched = append(matched, c)
            }
        }
    }
    if len(matched) > 0 {
        log.Printf("[AUDIT] IsUserInAdminRole: user=%s matched_roles=%v", userID, matched)
    }
    return len(matched) > 0, matched, nil
}

// IsAdminOverrideEnabled checks whether admin override is globally enabled
// Controlled by env var ENABLE_ADMIN_OVERRIDE=true
func IsAdminOverrideEnabled() bool {
    return strings.ToLower(strings.TrimSpace(os.Getenv("ENABLE_ADMIN_OVERRIDE"))) == "true"
}

// IsAdminUser returns true if the given userID is present in ADMIN_USER_IDS
func IsAdminUser(userID string) bool {
    if userID == "" {
        return false
    }
    loadAdminList()
    for _, id := range adminUserIDs {
        if id == userID {
            return true
        }
    }
    return false
}

// LoadEverythingIntoContext loads all approved master lists into a map that
// can be written into the request context by the middleware. It reuses the
// existing loadApproved* functions defined in the same package.
func LoadEverythingIntoContext(ctx context.Context, db *pgxpool.Pool) (map[string]interface{}, []string) {
    result := make(map[string]interface{})
    var errs []string

    banks, err := loadApprovedBanks(ctx, db)
    if err != nil {
        errs = append(errs, "loadApprovedBanks: "+err.Error())
        banks = []map[string]string{}
    }
    result["BankInfo"] = banks

    currencies, err := loadApprovedCurrencies(ctx, db)
    if err != nil {
        errs = append(errs, "loadApprovedCurrencies: "+err.Error())
        currencies = []map[string]string{}
    }
    result["ActiveCurrencies"] = currencies

    cashFlowCategories, err := loadApprovedCashFlowCategories(ctx, db)
    if err != nil {
        errs = append(errs, "loadApprovedCashFlowCategories: "+err.Error())
        cashFlowCategories = []map[string]string{}
    }
    result["CashFlowCategories"] = cashFlowCategories

    amcs, err := loadApprovedAMCs(ctx, db)
    if err != nil {
        errs = append(errs, "loadApprovedAMCs: "+err.Error())
        amcs = []map[string]string{}
    }
    result["ApprovedAMCs"] = amcs

    schemes, err := loadApprovedSchemes(ctx, db)
    if err != nil {
        errs = append(errs, "loadApprovedSchemes: "+err.Error())
        schemes = []map[string]string{}
    }
    result["ApprovedSchemes"] = schemes

    dps, err := loadApprovedDPs(ctx, db)
    if err != nil {
        errs = append(errs, "loadApprovedDPs: "+err.Error())
        dps = []map[string]string{}
    }
    result["ApprovedDPs"] = dps

    bankAccounts, err := loadApprovedBankAccounts(ctx, db)
    if err != nil {
        errs = append(errs, "loadApprovedBankAccounts: "+err.Error())
        bankAccounts = []map[string]string{}
    }
    result["ApprovedBankAccounts"] = bankAccounts

    folios, err := loadApprovedFolios(ctx, db)
    if err != nil {
        errs = append(errs, "loadApprovedFolios: "+err.Error())
        folios = []map[string]string{}
    }
    result["ApprovedFolios"] = folios

    demats, err := loadApprovedDemats(ctx, db)
    if err != nil {
        errs = append(errs, "loadApprovedDemats: "+err.Error())
        demats = []map[string]string{}
    }
    result["ApprovedDemats"] = demats

    return result, errs
}

// LoadAllEntities returns all known entity IDs/names (active, not deleted when possible).
// This is used for admin override so that entity-scoped queries can run across all entities.
func LoadAllEntities(ctx context.Context, db *pgxpool.Pool) ([]string, []string, error) {
    entityIDs := make([]string, 0)
    entityNames := make([]string, 0)
    seen := make(map[string]bool)

    type q struct {
        sql  string
        args []any
    }

    queries := []q{
        {
            sql: `
                SELECT entity_id::text, entity_name
                FROM masterentitycash
                WHERE COALESCE(is_deleted, false) = false
                  AND LOWER(active_status) = 'active'
                ORDER BY entity_name
            `,
        },
        {
            sql: `
                SELECT entity_id::text, entity_name
                FROM masterentitycash
                ORDER BY entity_name
            `,
        },
    }

    var lastErr error
    for _, qq := range queries {
        rows, err := db.Query(ctx, qq.sql, qq.args...)
        if err != nil {
            lastErr = err
            continue
        }
        for rows.Next() {
            var id, name string
            if err := rows.Scan(&id, &name); err == nil {
                if id != "" && !seen[id] {
                    seen[id] = true
                    entityIDs = append(entityIDs, id)
                    entityNames = append(entityNames, name)
                }
            }
        }
        rows.Close()
        lastErr = nil
        break
    }

    // Try to enrich with masterEntity if present
    queries2 := []q{
        {
            sql: `
                SELECT entity_id::text, entity_name
                FROM masterEntity
                WHERE COALESCE(is_deleted, false) = false
                  AND LOWER(active_status) = 'active'
                ORDER BY entity_name
            `,
        },
        {
            sql: `
                SELECT entity_id::text, entity_name
                FROM masterEntity
                ORDER BY entity_name
            `,
        },
    }

    for _, qq := range queries2 {
        rows, err := db.Query(ctx, qq.sql, qq.args...)
        if err != nil {
            // don't fail admin override if this table/schema differs
            continue
        }
        for rows.Next() {
            var id, name string
            if err := rows.Scan(&id, &name); err == nil {
                if id != "" && !seen[id] {
                    seen[id] = true
                    entityIDs = append(entityIDs, id)
                    entityNames = append(entityNames, name)
                }
            }
        }
        rows.Close()
        break
    }

    if len(entityIDs) == 0 {
        return nil, nil, lastErr
    }
    return entityIDs, entityNames, nil
}
