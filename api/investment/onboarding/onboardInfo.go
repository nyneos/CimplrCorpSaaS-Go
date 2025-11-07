package investment

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"CimplrCorpSaas/api"
)

// BatchInfo represents comprehensive batch information
type BatchInfo struct {
	BatchID       string                 `json:"batch_id"`
	UserID        string                 `json:"user_id"`
	UserEmail     string                 `json:"user_email"`
	Source        string                 `json:"source"`
	TotalRecords  int                    `json:"total_records"`
	Status        string                 `json:"status"`
	ApprovalStatus string                `json:"approval_status"`
	CreatedAt     time.Time              `json:"created_at"`
	CompletedAt   *time.Time             `json:"completed_at,omitempty"`
	Remarks       string                 `json:"remarks,omitempty"`
	
	// Detailed breakdown
	Entities      EntityBreakdown        `json:"entities"`
	Mappings      []OnboardMapping       `json:"mappings"`
	PortfolioMaps []PortfolioMap         `json:"portfolio_maps"`
	Snapshots     []PortfolioSnapshot    `json:"snapshots"`
	Transactions  []TransactionSummary   `json:"transactions"`
}

type EntityBreakdown struct {
	AMC    []AMCInfo    `json:"amc"`
	Scheme []SchemeInfo `json:"scheme"`
	DP     []DPInfo     `json:"dp"`
	Demat  []DematInfo  `json:"demat"`
	Folio  []FolioInfo  `json:"folio"`
}

type AMCInfo struct {
	AmcID             string `json:"amc_id"`
	AmcName           string `json:"amc_name"`
	InternalAmcCode   string `json:"internal_amc_code"`
	Status            string `json:"status"`
	IsDeleted         bool   `json:"is_deleted"`
}

type SchemeInfo struct {
	SchemeID           string `json:"scheme_id"`
	SchemeName         string `json:"scheme_name"`
	ISIN               string `json:"isin"`
	InternalSchemeCode string `json:"internal_scheme_code"`
	AmcName            string `json:"amc_name"`
	Status             string `json:"status"`
	IsDeleted          bool   `json:"is_deleted"`
}

type DPInfo struct {
	DPID       string `json:"dp_id"`
	DPName     string `json:"dp_name"`
	DPCode     string `json:"dp_code"`
	Depository string `json:"depository"`
	Status     string `json:"status"`
	IsDeleted  bool   `json:"is_deleted"`
}

type DematInfo struct {
	DematID              string `json:"demat_id"`
	EntityName           string `json:"entity_name"`
	DPID                 string `json:"dp_id"`
	Depository           string `json:"depository"`
	DematAccountNumber   string `json:"demat_account_number"`
	DefaultSettlementAcc string `json:"default_settlement_account"`
	Status               string `json:"status"`
	IsDeleted            bool   `json:"is_deleted"`
}

type FolioInfo struct {
	FolioID                    string `json:"folio_id"`
	EntityName                 string `json:"entity_name"`
	AmcName                    string `json:"amc_name"`
	FolioNumber                string `json:"folio_number"`
	FirstHolderName            string `json:"first_holder_name"`
	DefaultSubscriptionAccount string `json:"default_subscription_account"`
	DefaultRedemptionAccount   string `json:"default_redemption_account"`
	Status                     string `json:"status"`
	IsDeleted                  bool   `json:"is_deleted"`
}

type OnboardMapping struct {
	ID            string    `json:"id"`
	ReferenceID   string    `json:"reference_id"`
	ReferenceType string    `json:"reference_type"`
	ReferenceName string    `json:"reference_name"`
	Enriched      bool      `json:"enriched"`
	CreatedAt     time.Time `json:"created_at"`
}

type PortfolioMap struct {
	ID          int64  `json:"id"`
	EntityName  string `json:"entity_name"`
	FolioID     string `json:"folio_id,omitempty"`
	FolioNumber string `json:"folio_number,omitempty"`
	DematID     string `json:"demat_id,omitempty"`
	AmcID       string `json:"amc_id,omitempty"`
	SchemeID    string `json:"scheme_id,omitempty"`
	SchemeName  string `json:"scheme_name,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

type PortfolioSnapshot struct {
	ID           int64   `json:"id"`
	EntityName   string  `json:"entity_name"`
	FolioNumber  string  `json:"folio_number"`
	SchemeID     string  `json:"scheme_id,omitempty"`
	SchemeName   string  `json:"scheme_name,omitempty"`
	ISIN         string  `json:"isin,omitempty"`
	TotalUnits   float64 `json:"total_units"`
	AvgNav       float64 `json:"avg_nav"`
	CurrentNav   float64 `json:"current_nav"`
	CurrentValue float64 `json:"current_value"`
	GainLoss     float64 `json:"gain_loss"`
	CreatedAt    time.Time `json:"created_at"`
}

type TransactionSummary struct {
	ID                 int64     `json:"id"`
	TransactionDate    time.Time `json:"transaction_date"`
	TransactionType    string    `json:"transaction_type"`
	SchemeInternalCode string    `json:"scheme_internal_code"`
	FolioNumber        string    `json:"folio_number"`
	Amount             float64   `json:"amount"`
	Units              float64   `json:"units"`
	Nav                float64   `json:"nav"`
	CreatedAt          time.Time `json:"created_at"`
}

// BatchInfoRequest represents the request structure for batch info
type BatchInfoRequest struct {
	UserID  string `json:"user_id"`
	BatchID string `json:"batch_id"`
}

// GetBatchInfo retrieves comprehensive information about a specific batch
func GetBatchInfo(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		
		var req BatchInfoRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, "Invalid JSON: "+err.Error())
			return
		}
		
		if req.BatchID == "" {
			api.RespondWithError(w, 400, "batch_id required")
			return
		}
		
		if req.UserID == "" {
			api.RespondWithError(w, 400, "user_id required")
			return
		}
		
		batchID := req.BatchID

		// Get basic batch info
		batchInfo := BatchInfo{}
		var completedAt sql.NullTime
		var remarks sql.NullString
		
	 err := pgxPool.QueryRow(ctx, `
	     SELECT batch_id, user_id, user_email, source, total_records, status, approval_status,
		     created_at, completed_at, remarks
	     FROM investment.onboard_batch 
	     WHERE batch_id::text = $1::text`, batchID).Scan(
	     &batchInfo.BatchID, &batchInfo.UserID, &batchInfo.UserEmail,
	     &batchInfo.Source, &batchInfo.TotalRecords, &batchInfo.Status, &batchInfo.ApprovalStatus,
	     &batchInfo.CreatedAt, &completedAt, &remarks)
		
		if err != nil {
			if err == sql.ErrNoRows {
				api.RespondWithError(w, 404, "batch not found")
				return
			}
			api.RespondWithError(w, 500, "failed to get batch info: "+err.Error())
			return
		}
		
		if completedAt.Valid {
			batchInfo.CompletedAt = &completedAt.Time
		}
		if remarks.Valid {
			batchInfo.Remarks = remarks.String
		}

		// Get entities breakdown
		batchInfo.Entities = EntityBreakdown{}
		
		// Get AMCs
		amcRows, err := pgxPool.Query(ctx, `
			SELECT amc_id, amc_name, internal_amc_code, status, COALESCE(is_deleted, false)
			FROM investment.masteramc 
			WHERE batch_id::text = $1::text
			ORDER BY amc_name`, batchID)
		if err == nil {
			for amcRows.Next() {
				var amc AMCInfo
				amcRows.Scan(&amc.AmcID, &amc.AmcName, &amc.InternalAmcCode, &amc.Status, &amc.IsDeleted)
				batchInfo.Entities.AMC = append(batchInfo.Entities.AMC, amc)
			}
			amcRows.Close()
		}

		// Get Schemes
		schemeRows, err := pgxPool.Query(ctx, `
			SELECT scheme_id, scheme_name, isin, internal_scheme_code, amc_name, status, COALESCE(is_deleted, false)
			FROM investment.masterscheme 
			WHERE batch_id::text = $1::text
			ORDER BY scheme_name`, batchID)
		if err == nil {
			for schemeRows.Next() {
				var scheme SchemeInfo
				schemeRows.Scan(&scheme.SchemeID, &scheme.SchemeName, &scheme.ISIN, 
					&scheme.InternalSchemeCode, &scheme.AmcName, &scheme.Status, &scheme.IsDeleted)
				batchInfo.Entities.Scheme = append(batchInfo.Entities.Scheme, scheme)
			}
			schemeRows.Close()
		}

		// Get DPs
		dpRows, err := pgxPool.Query(ctx, `
			SELECT dp_id, dp_name, dp_code, depository, status, COALESCE(is_deleted, false)
			FROM investment.masterdepositoryparticipant 
			WHERE batch_id::text = $1::text
			ORDER BY dp_name`, batchID)
		if err == nil {
			for dpRows.Next() {
				var dp DPInfo
				dpRows.Scan(&dp.DPID, &dp.DPName, &dp.DPCode, &dp.Depository, &dp.Status, &dp.IsDeleted)
				batchInfo.Entities.DP = append(batchInfo.Entities.DP, dp)
			}
			dpRows.Close()
		}

		// Get Demats
		dematRows, err := pgxPool.Query(ctx, `
			SELECT demat_id, entity_name, dp_id, depository, demat_account_number, 
			       default_settlement_account, status, COALESCE(is_deleted, false)
			FROM investment.masterdemataccount 
			WHERE batch_id::text = $1::text
			ORDER BY entity_name`, batchID)
		if err == nil {
			for dematRows.Next() {
				var demat DematInfo
				dematRows.Scan(&demat.DematID, &demat.EntityName, &demat.DPID, &demat.Depository,
					&demat.DematAccountNumber, &demat.DefaultSettlementAcc, &demat.Status, &demat.IsDeleted)
				batchInfo.Entities.Demat = append(batchInfo.Entities.Demat, demat)
			}
			dematRows.Close()
		}

		// Get Folios
		folioRows, err := pgxPool.Query(ctx, `
			SELECT folio_id, entity_name, amc_name, folio_number, first_holder_name,
			       default_subscription_account, default_redemption_account, status, COALESCE(is_deleted, false)
			FROM investment.masterfolio 
			WHERE batch_id::text = $1::text
			ORDER BY entity_name, folio_number`, batchID)
		if err == nil {
			for folioRows.Next() {
				var folio FolioInfo
				folioRows.Scan(&folio.FolioID, &folio.EntityName, &folio.AmcName, &folio.FolioNumber,
					&folio.FirstHolderName, &folio.DefaultSubscriptionAccount, 
					&folio.DefaultRedemptionAccount, &folio.Status, &folio.IsDeleted)
				batchInfo.Entities.Folio = append(batchInfo.Entities.Folio, folio)
			}
			folioRows.Close()
		}

		// Get Onboard Mappings
		mappingRows, err := pgxPool.Query(ctx, `
			SELECT id, reference_id, reference_type, reference_name, enriched, created_at
			FROM investment.onboard_mapping 
			WHERE batch_id::text = $1::text
			ORDER BY reference_type, reference_name`, batchID)
		if err == nil {
			for mappingRows.Next() {
				var mapping OnboardMapping
				mappingRows.Scan(&mapping.ID, &mapping.ReferenceID, &mapping.ReferenceType,
					&mapping.ReferenceName, &mapping.Enriched, &mapping.CreatedAt)
				batchInfo.Mappings = append(batchInfo.Mappings, mapping)
			}
			mappingRows.Close()
		}

		// Get Portfolio Maps
		portfolioRows, err := pgxPool.Query(ctx, `
			SELECT id, entity_name, COALESCE(folio_id,''), COALESCE(folio_number,''), 
			       COALESCE(demat_id,''), COALESCE(amc_id,''), COALESCE(scheme_id,''), 
			       COALESCE(scheme_name,''), created_at
			FROM investment.portfolio_onboarding_map 
			WHERE batch_id::text = $1::text
			ORDER BY entity_name`, batchID)
		if err == nil {
			for portfolioRows.Next() {
				var portfolio PortfolioMap
				portfolioRows.Scan(&portfolio.ID, &portfolio.EntityName, &portfolio.FolioID,
					&portfolio.FolioNumber, &portfolio.DematID, &portfolio.AmcID,
					&portfolio.SchemeID, &portfolio.SchemeName, &portfolio.CreatedAt)
				batchInfo.PortfolioMaps = append(batchInfo.PortfolioMaps, portfolio)
			}
			portfolioRows.Close()
		}

		// Get Portfolio Snapshots
		snapshotRows, err := pgxPool.Query(ctx, `
			SELECT id, entity_name, folio_number, COALESCE(scheme_id,''), COALESCE(scheme_name,''),
			       COALESCE(isin,''), total_units, avg_nav, current_nav, current_value, gain_loss, created_at
			FROM investment.portfolio_snapshot 
			WHERE batch_id::text = $1::text
			ORDER BY entity_name, folio_number`, batchID)
		if err == nil {
			for snapshotRows.Next() {
				var snapshot PortfolioSnapshot
				snapshotRows.Scan(&snapshot.ID, &snapshot.EntityName, &snapshot.FolioNumber,
					&snapshot.SchemeID, &snapshot.SchemeName, &snapshot.ISIN,
					&snapshot.TotalUnits, &snapshot.AvgNav, &snapshot.CurrentNav,
					&snapshot.CurrentValue, &snapshot.GainLoss, &snapshot.CreatedAt)
				batchInfo.Snapshots = append(batchInfo.Snapshots, snapshot)
			}
			snapshotRows.Close()
		}

		// Get Transactions
		txRows, err := pgxPool.Query(ctx, `
			SELECT id, transaction_date, transaction_type, scheme_internal_code, folio_number,
			       amount, units, nav, created_at
			FROM investment.onboard_transaction 
			WHERE batch_id::text = $1::text
			ORDER BY transaction_date DESC`, batchID)
		if err == nil {
			for txRows.Next() {
				var tx TransactionSummary
				txRows.Scan(&tx.ID, &tx.TransactionDate, &tx.TransactionType,
					&tx.SchemeInternalCode, &tx.FolioNumber, &tx.Amount,
					&tx.Units, &tx.Nav, &tx.CreatedAt)
				batchInfo.Transactions = append(batchInfo.Transactions, tx)
			}
			txRows.Close()
		}

		api.RespondWithPayload(w, true, "", batchInfo)
	}
}

// BatchListRequest represents the request structure for batch list
type BatchListRequest struct {
	UserID string `json:"user_id"`
}

// GetAllBatches retrieves a list of all batches with summary information
func GetAllBatches(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		
		var req BatchListRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, "Invalid JSON: "+err.Error())
			return
		}
		
		if req.UserID == "" {
			api.RespondWithError(w, 400, "user_id required")
			return
		}
		
		type BatchSummary struct {
			BatchID      string     `json:"batch_id"`
			UserID       string     `json:"user_id"`
			UserEmail    string     `json:"user_email"`
			Source       string     `json:"source"`
			TotalRecords int        `json:"total_records"`
			Status       string     `json:"status"`
			ApprovalStatus string   `json:"approval_status"`
			CreatedAt    time.Time  `json:"created_at"`
			CompletedAt  *time.Time `json:"completed_at,omitempty"`
			EntityCounts map[string]int `json:"entity_counts"`
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT ob.batch_id, ob.user_id, ob.user_email, ob.source, ob.total_records, 
			       ob.status, ob.approval_status, ob.created_at, ob.completed_at,
			       (SELECT COUNT(*) FROM investment.masteramc WHERE batch_id::text = ob.batch_id::text) as amc_count,
			       (SELECT COUNT(*) FROM investment.masterscheme WHERE batch_id::text = ob.batch_id::text) as scheme_count,
			       (SELECT COUNT(*) FROM investment.masterdepositoryparticipant WHERE batch_id::text = ob.batch_id::text) as dp_count,
			       (SELECT COUNT(*) FROM investment.masterdemataccount WHERE batch_id::text = ob.batch_id::text) as demat_count,
			       (SELECT COUNT(*) FROM investment.masterfolio WHERE batch_id::text = ob.batch_id::text) as folio_count
			FROM investment.onboard_batch ob
			ORDER BY ob.created_at DESC
			LIMIT 100`)
		
		if err != nil {
			api.RespondWithError(w, 500, "failed to get batches: "+err.Error())
			return
		}
		defer rows.Close()

		var batches []BatchSummary
		for rows.Next() {
			var batch BatchSummary
			var completedAt sql.NullTime
			var amcCount, schemeCount, dpCount, dematCount, folioCount int

			err = rows.Scan(&batch.BatchID, &batch.UserID, &batch.UserEmail, &batch.Source,
				&batch.TotalRecords, &batch.Status, &batch.ApprovalStatus, &batch.CreatedAt, &completedAt,
				&amcCount, &schemeCount, &dpCount, &dematCount, &folioCount)
			if err != nil {
				continue
			}
			
			if completedAt.Valid {
				batch.CompletedAt = &completedAt.Time
			}
			
			batch.EntityCounts = map[string]int{
				"amc":    amcCount,
				"scheme": schemeCount,
				"dp":     dpCount,
				"demat":  dematCount,
				"folio":  folioCount,
			}
			
			batches = append(batches, batch)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"batches": batches,
			"total":   len(batches),
		})
	}
}
