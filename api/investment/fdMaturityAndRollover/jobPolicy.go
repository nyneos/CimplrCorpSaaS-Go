package fdMaturityAndRollover

import (
	"context"
	"fmt"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const JobPathAutoMaturity = "job://investment/auto-maturity"

// EnforceAutoMaturityConfirm gates auto-maturity confirm creation.
func EnforceAutoMaturityConfirm(ctx context.Context, pool *pgxpool.Pool, initiateID, fdID, closureType, entityID string) (bool, string) {
	fields := map[string]interface{}{
		"closure_initiate_id": initiateID,
		"fd_id":               fdID,
		"closure_type":        closureType,
		"execution_mode":      "CRON",
	}
	if row, err := loadFDClosureInitiateRow(ctx, pool, initiateID); err == nil {
		for k, v := range buildFDClosureInitiatePolicyFields(row) {
			fields[k] = v
		}
		if entityID == "" {
			entityID = row.EntityID
		}
	}
	return runtime.EnforceJobInline(ctx, pool, runtime.JobEnforceInput{
		EventCode:           common.TriggerWorkflowTransition,
		ModuleCode:          common.ModuleInvestmentFD,
		SubModule:           "FD_CLOSURE",
		EntityCode:          entityID,
		ActorUserID:         "SYSTEM:auto-maturity",
		HandlerName:         "processCimplrAutoMaturityInitiate",
		APIPath:             JobPathAutoMaturity,
		CorrelationID:       fmt.Sprintf("AUTO-MAT-%s", initiateID),
		BusinessRecordID:    initiateID,
		BusinessRecordType:  "FD_CLOSURE_INITIATE",
		Fields:              fields,
		DefaultBlockMessage: "Auto maturity confirm blocked by policy",
	})
}
