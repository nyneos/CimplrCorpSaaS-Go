package counterpartyHub

import (
	middlewares "CimplrCorpSaas/api/middlewares"
	"database/sql"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterCounterpartyHubRoutes wires all /master/v2/counterparty-hub/* routes.
// Call this from master.StartMasterService after the existing counterparty block.
func RegisterCounterpartyHubRoutes(mux *http.ServeMux, pool *pgxpool.Pool, db *sql.DB) {
	pre := middlewares.PreValidationMiddleware(pool)

	// ── Hub v2 unified routes (/master/v2/counterparty-hub/*) ─────────────────
	// Single payload; counterparty_type routes every write to the correct typed master.
	mux.Handle("/master/v2/counterparty-hub/create", pre(http.HandlerFunc(CreateCounterparty(pool))))
	mux.Handle("/master/v2/counterparty-hub/create-bulk", pre(http.HandlerFunc(CreateCounterpartyBulk(pool))))
	mux.Handle("/master/v2/counterparty-hub/update", pre(http.HandlerFunc(UpdateCounterparty(pool))))
	mux.Handle("/master/v2/counterparty-hub/bulk-approve", pre(http.HandlerFunc(BulkApproveCounterparty(pool))))
	mux.Handle("/master/v2/counterparty-hub/bulk-reject", pre(http.HandlerFunc(BulkRejectCounterparty(pool))))
	mux.Handle("/master/v2/counterparty-hub/bulk-delete", pre(http.HandlerFunc(BulkDeleteCounterparty(pool))))
	mux.Handle("/master/v2/counterparty-hub/all", pre(http.HandlerFunc(GetCounterpartyAll(pool))))
	mux.Handle("/master/v2/counterparty-hub/detail", pre(http.HandlerFunc(GetCounterpartyDetail(pool))))
	mux.Handle("/master/v2/counterparty-hub/audit-history", pre(http.HandlerFunc(GetCounterpartyAuditHistory(pool))))
	mux.Handle("/master/v2/counterparty-hub/approved-active", pre(http.HandlerFunc(GetCounterpartyApprovedActive(pool))))

	// ── Bulk Upload v2 (apibox_svc schema, unified flat payload) ─────────────
	mux.Handle("/master/v2/counterparty-hub/upload", pre(UploadCounterpartyHubV2(pool)))
	mux.Handle("/master/v2/counterparty-hub/upload/template", pre(GetUploadTemplateV2(pool)))

	// ── Additional Files (DMS / S3 document attachments per counterparty) ────
	counterpartyHubMF := CounterpartyHubFilesHandlers(pool)
	mux.Handle("/master/v2/counterparty-hub/additional-files/list", pre(counterpartyHubMF.List))
	mux.Handle("/master/v2/counterparty-hub/additional-files/upload", pre(counterpartyHubMF.Upload))
	mux.Handle("/master/v2/counterparty-hub/additional-files/download", pre(counterpartyHubMF.Download))
	mux.Handle("/master/v2/counterparty-hub/additional-files/download-bulk", pre(counterpartyHubMF.DownloadBulk))
	mux.Handle("/master/v2/counterparty-hub/additional-files/delete", pre(counterpartyHubMF.Delete))

	// ── Per-type handlers below are superseded by the unified hub v2 routes ──
	// They remain compiled but are not registered. Remove the comment prefix
	// to re-enable individual type routes if needed for legacy compatibility.

	// Counterparty Master (parent table)
	// mux.Handle("/master/v2/counterparty-hub/counterparty/create", pre(CreateCounterpartyMaster(pool)))
	// mux.Handle("/master/v2/counterparty-hub/counterparty/bulk-create", pre(CreateCounterpartyMasterBulk(pool)))
	// mux.Handle("/master/v2/counterparty-hub/counterparty/update", pre(UpdateCounterpartyMaster(pool)))
	// mux.Handle("/master/v2/counterparty-hub/counterparty/bulk-approve", pre(BulkApproveCounterpartyMaster(pool)))
	// mux.Handle("/master/v2/counterparty-hub/counterparty/bulk-reject", pre(BulkRejectCounterpartyMaster(pool)))
	// mux.Handle("/master/v2/counterparty-hub/counterparty/bulk-delete", pre(BulkDeleteCounterpartyMaster(pool)))
	// mux.Handle("/master/v2/counterparty-hub/counterparty/all", pre(GetCounterpartyMasterAll(pool)))
	// mux.Handle("/master/v2/counterparty-hub/counterparty/audit-history", pre(GetCounterpartyMasterAuditHistory(pool)))
	// mux.Handle("/master/v2/counterparty-hub/counterparty/approved-active", pre(GetCounterpartyMasterApprovedActive(pool)))
	// mux.Handle("/master/v2/counterparty-hub/counterparty/detail", pre(GetCounterpartyMasterDetail(pool)))

	// Bank Master
	// mux.Handle("/master/v2/counterparty-hub/bank/create", pre(CreateBank(pool)))
	// mux.Handle("/master/v2/counterparty-hub/bank/create-bulk", pre(CreateBankBulk(pool)))
	// mux.Handle("/master/v2/counterparty-hub/bank/update", pre(UpdateBank(pool)))
	// mux.Handle("/master/v2/counterparty-hub/bank/bulk-approve", pre(BulkApproveBank(pool)))
	// mux.Handle("/master/v2/counterparty-hub/bank/bulk-reject", pre(BulkRejectBank(pool)))
	// mux.Handle("/master/v2/counterparty-hub/bank/bulk-delete", pre(BulkDeleteBank(pool)))
	// mux.Handle("/master/v2/counterparty-hub/bank/all", pre(GetBankAll(pool)))
	// mux.Handle("/master/v2/counterparty-hub/bank/audit-history", pre(GetBankAuditHistory(pool)))
	// mux.Handle("/master/v2/counterparty-hub/bank/approved-active", pre(GetBankApprovedActive(pool)))
	// mux.Handle("/master/v2/counterparty-hub/bank/detail", pre(GetBankDetail(pool)))

	// Exchange Master
	// mux.Handle("/master/v2/counterparty-hub/exchange/create", pre(CreateExchange(pool)))
	// mux.Handle("/master/v2/counterparty-hub/exchange/bulk-create", pre(CreateExchangeBulk(pool)))
	// mux.Handle("/master/v2/counterparty-hub/exchange/update", pre(UpdateExchange(pool)))
	// mux.Handle("/master/v2/counterparty-hub/exchange/bulk-approve", pre(BulkApproveExchange(pool)))
	// mux.Handle("/master/v2/counterparty-hub/exchange/bulk-reject", pre(BulkRejectExchange(pool)))
	// mux.Handle("/master/v2/counterparty-hub/exchange/bulk-delete", pre(BulkDeleteExchange(pool)))
	// mux.Handle("/master/v2/counterparty-hub/exchange/all", pre(GetExchangeAll(pool)))
	// mux.Handle("/master/v2/counterparty-hub/exchange/audit-history", pre(GetExchangeAuditHistory(pool)))
	// mux.Handle("/master/v2/counterparty-hub/exchange/approved-active", pre(GetExchangeApprovedActive(pool)))
	// mux.Handle("/master/v2/counterparty-hub/exchange/detail", pre(GetExchangeDetail(pool)))

	// Data Provider Master
	// mux.Handle("/master/v2/counterparty-hub/data-provider/create", pre(CreateDataProvider(pool)))
	// mux.Handle("/master/v2/counterparty-hub/data-provider/bulk-create", pre(CreateDataProviderBulk(pool)))
	// mux.Handle("/master/v2/counterparty-hub/data-provider/update", pre(UpdateDataProvider(pool)))
	// mux.Handle("/master/v2/counterparty-hub/data-provider/bulk-approve", pre(BulkApproveDataProvider(pool)))
	// mux.Handle("/master/v2/counterparty-hub/data-provider/bulk-reject", pre(BulkRejectDataProvider(pool)))
	// mux.Handle("/master/v2/counterparty-hub/data-provider/bulk-delete", pre(BulkDeleteDataProvider(pool)))
	// mux.Handle("/master/v2/counterparty-hub/data-provider/all", pre(GetDataProviderAll(pool)))
	// mux.Handle("/master/v2/counterparty-hub/data-provider/audit-history", pre(GetDataProviderAuditHistory(pool)))
	// mux.Handle("/master/v2/counterparty-hub/data-provider/approved-active", pre(GetDataProviderApprovedActive(pool)))
	// mux.Handle("/master/v2/counterparty-hub/data-provider/detail", pre(GetDataProviderDetail(pool)))

	// CCP / CSD Master
	// mux.Handle("/master/v2/counterparty-hub/ccp-csd/create", pre(CreateCcpCsd(pool)))
	// mux.Handle("/master/v2/counterparty-hub/ccp-csd/bulk-create", pre(CreateCcpCsdBulk(pool)))
	// mux.Handle("/master/v2/counterparty-hub/ccp-csd/update", pre(UpdateCcpCsd(pool)))
	// mux.Handle("/master/v2/counterparty-hub/ccp-csd/bulk-approve", pre(BulkApproveCcpCsd(pool)))
	// mux.Handle("/master/v2/counterparty-hub/ccp-csd/bulk-reject", pre(BulkRejectCcpCsd(pool)))
	// mux.Handle("/master/v2/counterparty-hub/ccp-csd/bulk-delete", pre(BulkDeleteCcpCsd(pool)))
	// mux.Handle("/master/v2/counterparty-hub/ccp-csd/all", pre(GetCcpCsdAll(pool)))
	// mux.Handle("/master/v2/counterparty-hub/ccp-csd/audit-history", pre(GetCcpCsdAuditHistory(pool)))
	// mux.Handle("/master/v2/counterparty-hub/ccp-csd/approved-active", pre(GetCcpCsdApprovedActive(pool)))
	// mux.Handle("/master/v2/counterparty-hub/ccp-csd/detail", pre(GetCcpCsdDetail(pool)))

	// Payment Network Master
	// mux.Handle("/master/v2/counterparty-hub/payment-network/create", pre(CreatePaymentNetwork(pool)))
	// mux.Handle("/master/v2/counterparty-hub/payment-network/bulk-create", pre(CreatePaymentNetworkBulk(pool)))
	// mux.Handle("/master/v2/counterparty-hub/payment-network/update", pre(UpdatePaymentNetwork(pool)))
	// mux.Handle("/master/v2/counterparty-hub/payment-network/bulk-approve", pre(BulkApprovePaymentNetwork(pool)))
	// mux.Handle("/master/v2/counterparty-hub/payment-network/bulk-reject", pre(BulkRejectPaymentNetwork(pool)))
	// mux.Handle("/master/v2/counterparty-hub/payment-network/bulk-delete", pre(BulkDeletePaymentNetwork(pool)))
	// mux.Handle("/master/v2/counterparty-hub/payment-network/all", pre(GetPaymentNetworkAll(pool)))
	// mux.Handle("/master/v2/counterparty-hub/payment-network/audit-history", pre(GetPaymentNetworkAuditHistory(pool)))
	// mux.Handle("/master/v2/counterparty-hub/payment-network/approved-active", pre(GetPaymentNetworkApprovedActive(pool)))
	// mux.Handle("/master/v2/counterparty-hub/payment-network/detail", pre(GetPaymentNetworkDetail(pool)))

	// ERP System Master
	// mux.Handle("/master/v2/counterparty-hub/erp-system/create", pre(CreateERPSystem(pool)))
	// mux.Handle("/master/v2/counterparty-hub/erp-system/bulk-create", pre(CreateERPSystemBulk(pool)))
	// mux.Handle("/master/v2/counterparty-hub/erp-system/update", pre(UpdateERPSystem(pool)))
	// mux.Handle("/master/v2/counterparty-hub/erp-system/bulk-approve", pre(BulkApproveERPSystem(pool)))
	// mux.Handle("/master/v2/counterparty-hub/erp-system/bulk-reject", pre(BulkRejectERPSystem(pool)))
	// mux.Handle("/master/v2/counterparty-hub/erp-system/bulk-delete", pre(BulkDeleteERPSystem(pool)))
	// mux.Handle("/master/v2/counterparty-hub/erp-system/all", pre(GetERPSystemAll(pool)))
	// mux.Handle("/master/v2/counterparty-hub/erp-system/audit-history", pre(GetERPSystemAuditHistory(pool)))
	// mux.Handle("/master/v2/counterparty-hub/erp-system/approved-active", pre(GetERPSystemApprovedActive(pool)))
	// mux.Handle("/master/v2/counterparty-hub/erp-system/detail", pre(GetERPSystemDetail(pool)))
}
