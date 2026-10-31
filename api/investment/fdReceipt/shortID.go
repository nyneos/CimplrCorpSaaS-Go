package fdReceipt

import "fmt"

func freeShortIDExpr(prefix, table, column string) string {
	return fmt.Sprintf(`(SELECT c.id FROM (
		SELECT '%s' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,7)) AS id
		FROM generate_series(1,8)
	) c WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE t.%s = c.id) LIMIT 1)`, prefix, table, column)
}
