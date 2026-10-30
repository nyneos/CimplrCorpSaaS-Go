package fdReceipt

import "fmt"

// freeShortIDExpr returns a SQL scalar expression that yields prefix + 7 random hex
// characters not already used in table.column. Several candidates are drawn so a
// collision in the 28-bit space picks the next one instead of failing the insert.
// table and column must be compile-time constants, never request data.
func freeShortIDExpr(prefix, table, column string) string {
	return fmt.Sprintf(`(SELECT c.id FROM (
		SELECT '%s' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,7)) AS id
		FROM generate_series(1,8)
	) c WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE t.%s = c.id) LIMIT 1)`, prefix, table, column)
}
