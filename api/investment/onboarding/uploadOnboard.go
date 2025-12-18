package investment

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"

	// "context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

<<<<<<< HEAD
	"CimplrCorpSaas/api/constants"

=======
>>>>>>> origin/main
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Input DTOs - frontend should send these fields (no old_*, no is_deleted)
type AMCInput struct {
	Enriched            bool   `json:"enriched"`
	AmcID               string `json:"amc_id,omitempty"`
	AmcName             string `json:"amc_name"`
	InternalAmcCode     string `json:"internal_amc_code"`
	Status              string `json:"status,omitempty"`
	PrimaryContactName  string `json:"primary_contact_name,omitempty"`
	PrimaryContactEmail string `json:"primary_contact_email,omitempty"`
	SebiRegistrationNo  string `json:"sebi_registration_no,omitempty"`
	AmcBeneficiaryName  string `json:"amc_beneficiary_name,omitempty"`
	AmcBankAccountNo    string `json:"amc_bank_account_no,omitempty"`
	AmcBankName         string `json:"amc_bank_name,omitempty"`
	AmcBankIfsc         string `json:"amc_bank_ifsc,omitempty"`
	MfuAmcCode          string `json:"mfu_amc_code,omitempty"`
	CamsAmcCode         string `json:"cams_amc_code,omitempty"`
	ErpVendorCode       string `json:"erp_vendor_code,omitempty"`
}

type SchemeInput struct {
	Enriched           bool   `json:"enriched"`
	SchemeID           string `json:"scheme_id,omitempty"`
	SchemeName         string `json:"scheme_name"`
	ISIN               string `json:"isin"`
	AmcName            string `json:"amc_name"`
	InternalSchemeCode string `json:"internal_scheme_code"`
	InternalRiskRating string `json:"internal_risk_rating,omitempty"`
	ErpGlAccount       string `json:"erp_gl_account,omitempty"`
	Status             string `json:"status,omitempty"`
}

type DPInput struct {
	Enriched   bool   `json:"enriched"`
	DPID       string `json:"dp_id,omitempty"`
	DPName     string `json:"dp_name"`
	DPCode     string `json:"dp_code"`
	Depository string `json:"depository"`
	Status     string `json:"status,omitempty"`
}

type DematInput struct {
	Enriched                 bool   `json:"enriched"`
	DematID                  string `json:"demat_id,omitempty"`
	EntityName               string `json:"entity_name"`
	DPID                     string `json:"dp_id"`
	Depository               string `json:"depository"`
	DematAccountNumber       string `json:"demat_account_number"`
	DepositoryParticipant    string `json:"depository_participant,omitempty"`
	ClientID                 string `json:"client_id,omitempty"`
	DefaultSettlementAccount string `json:"default_settlement_account"`
	Status                   string `json:"status,omitempty"`
}

type FolioInput struct {
	Enriched                   bool     `json:"enriched"`
	FolioID                    string   `json:"folio_id,omitempty"`
	EntityName                 string   `json:"entity_name"`
	AmcName                    string   `json:"amc_name"`
	FolioNumber                string   `json:"folio_number"`
	FirstHolderName            string   `json:"first_holder_name"`
	DefaultSubscriptionAccount string   `json:"default_subscription_account"`
	DefaultRedemptionAccount   string   `json:"default_redemption_account"`
	Status                     string   `json:"status,omitempty"`
	SchemeRefs                 []string `json:"scheme_ids,omitempty"` // optional scheme refs to map
}

// Transaction CSV row
type TxCSVRow struct {
	TransactionDate    time.Time
	TransactionType    string
	SchemeInternalCode string
	FolioNumber        string
	DematAccNumber     string
	Amount             float64
	Units              float64
	Nav                float64
}

// Response structure
type UploadResponse struct {
<<<<<<< HEAD
	Success bool             `json:"success"`
	BatchID string           `json:"batch_id,omitempty"`
	Counts  map[string]int64 `json:"counts,omitempty"`
	Message string           `json:"message,omitempty"`
=======
	Success        bool             `json:"success"`
	BatchID        string           `json:"batch_id,omitempty"`
	Counts         map[string]int64 `json:"counts,omitempty"`
	EnrichedCounts map[string]int64 `json:"enriched_counts,omitempty"`
	Message        string           `json:"message,omitempty"`
>>>>>>> origin/main
}

// helper
func defaultIfEmpty(val, def string) string {
	if strings.TrimSpace(val) == "" {
		return def
	}
	return val
}

// parse JSON array field from multipart form; field should be a single JSON array string
func parseJSONField[T any](mf *multipart.Form, key string) ([]T, error) {
	if mf == nil {
		return nil, nil
	}
	vals := mf.Value[key]
	if len(vals) == 0 {
		return nil, nil
	}
	var out []T
	if err := json.Unmarshal([]byte(vals[0]), &out); err != nil {
		return nil, err
	}
	return out, nil
}

// parse transactions CSV
func parseTransactionsCSV(r io.Reader) ([]TxCSVRow, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true
	rows := []TxCSVRow{}
	hdr, err := cr.Read()
	if err != nil {
		return nil, err
	}
	_ = hdr
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) < 7 {
			return nil, fmt.Errorf("csv: expected >=7 cols, got %d", len(rec))
		}
		// parse date using NormalizeDate
		dtStr := strings.TrimSpace(rec[0])
		normalizedDate, err := NormalizeDate(dtStr)
		if err != nil {
			return nil, fmt.Errorf("invalid date %s: %v", dtStr, err)
		}
<<<<<<< HEAD
		dt, err := time.Parse(constants.DateFormat, normalizedDate)
		if err != nil {
			return nil, fmt.Errorf("failed to parse normalized date %s: %v", normalizedDate, err)
		}
		// Handle both folio and demat (8 cols) or just folio (7 cols)
		folioNumber := strings.TrimSpace(rec[3])
		dematAccNumber := ""
		amountIdx := 4
		if len(rec) >= 8 {
			dematAccNumber = strings.TrimSpace(rec[4])
			amountIdx = 5
=======
		dt, err := time.Parse("2006-01-02", normalizedDate)
		if err != nil {
			return nil, fmt.Errorf("failed to parse normalized date %s: %v", normalizedDate, err)
		}
		// Handle both folio and demat
		// CSV format: TransactionDate,TransactionType,SchemeInternalCode,FolioNumber,Amount,Units,NAV,DematAccNumber
		// So: rec[0]=date, rec[1]=type, rec[2]=scheme, rec[3]=folio, rec[4]=amount, rec[5]=units, rec[6]=nav, rec[7]=demat (if present)
		folioNumber := strings.TrimSpace(rec[3])
		dematAccNumber := ""
		if len(rec) >= 8 {
			dematAccNumber = strings.TrimSpace(rec[7]) // DematAccNumber is at the END of the row
>>>>>>> origin/main
		}
		// Validate: either folio or demat must be present
		if folioNumber == "" && dematAccNumber == "" {
			return nil, fmt.Errorf("either folio_number or demat_acc_number must be provided")
		}
<<<<<<< HEAD
		amount, _ := strconv.ParseFloat(strings.TrimSpace(rec[amountIdx]), 64)
		units, _ := strconv.ParseFloat(strings.TrimSpace(rec[amountIdx+1]), 64)
		nav, _ := strconv.ParseFloat(strings.TrimSpace(rec[amountIdx+2]), 64)
=======
		amount, _ := strconv.ParseFloat(strings.TrimSpace(rec[4]), 64)
		units, _ := strconv.ParseFloat(strings.TrimSpace(rec[5]), 64)
		nav, _ := strconv.ParseFloat(strings.TrimSpace(rec[6]), 64)
>>>>>>> origin/main
		rows = append(rows, TxCSVRow{TransactionDate: dt, TransactionType: strings.TrimSpace(rec[1]), SchemeInternalCode: strings.TrimSpace(rec[2]), FolioNumber: folioNumber, DematAccNumber: dematAccNumber, Amount: amount, Units: units, Nav: nav})
	}
	return rows, nil
}

// Main handler
func UploadInvestmentBulkk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		log.Printf("[bulk] start processing upload")

		// parse multipart
		if err := r.ParseMultipartForm(100 << 20); err != nil {
			log.Printf("[bulk] parse multipart: %v", err)
			api.RespondWithError(w, http.StatusBadRequest, "form parse failed: "+err.Error())
			return
		}

<<<<<<< HEAD
		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
=======
		userID := r.FormValue("user_id")
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id required")
>>>>>>> origin/main
			return
		}
		// resolve user email from active sessions
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
<<<<<<< HEAD
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
=======
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
>>>>>>> origin/main
			return
		}
		log.Printf("[bulk] user %s (%s)", userID, userEmail)

		// begin tx (SERIALIZABLE)
		tx, err := pgxPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			log.Printf("[bulk] tx begin: %v", err)
<<<<<<< HEAD
			api.RespondWithError(w, 500, constants.ErrTxBegin+err.Error())
=======
			api.RespondWithError(w, 500, "tx begin: "+err.Error())
>>>>>>> origin/main
			return
		}
		defer func() {
			if tx != nil {
				_ = tx.Rollback(ctx)
				log.Printf("[bulk] tx rolled back in defer")
			}
		}()

		// get or create batch
		var batchID string
		providedBatchID := r.FormValue("batch_id")
		if providedBatchID != "" {
			// Check if batch already exists
			err := tx.QueryRow(ctx, `SELECT batch_id FROM investment.onboard_batch WHERE batch_id::text = $1`, providedBatchID).Scan(&batchID)
			if err != nil {
				// Create new batch with provided ID (convert string to UUID)
				if err := tx.QueryRow(ctx, `INSERT INTO investment.onboard_batch (batch_id, user_id, user_email, source, total_records, status) VALUES ($1::uuid, $2, $3, 'Workbench', 0, 'IN_PROGRESS') RETURNING batch_id`, providedBatchID, userID, userEmail).Scan(&batchID); err != nil {
					log.Printf("[bulk] create batch with provided ID failed: %v", err)
					api.RespondWithError(w, 500, "create batch failed: "+err.Error())
					return
				}
			}
		} else {
			// Create new batch with auto-generated ID
			if err := tx.QueryRow(ctx, `INSERT INTO investment.onboard_batch (user_id, user_email, source, total_records, status) VALUES ($1,$2,'Workbench',0,'IN_PROGRESS') RETURNING batch_id`, userID, userEmail).Scan(&batchID); err != nil {
				log.Printf("[bulk] create batch failed: %v", err)
				api.RespondWithError(w, 500, "create batch failed: "+err.Error())
				return
			}
		}
		log.Printf("[bulk] using batch %s", batchID)
		var amcs []AMCInput
		var schemes []SchemeInput
		var dps []DPInput
		var demats []DematInput
		var folios []FolioInput

		if fhArr := r.MultipartForm.File["payload"]; len(fhArr) > 0 {
			payloadFile, err := fhArr[0].Open()
			if err != nil {
				log.Printf("[bulk] failed to open payload file: %v", err)
			} else {
				var payload struct {
					AMC    []AMCInput    `json:"amc"`
					Scheme []SchemeInput `json:"scheme"`
					DP     []DPInput     `json:"dp"`
					Demat  []DematInput  `json:"demat"`
					Folio  []FolioInput  `json:"folio"`
				}
				if err := json.NewDecoder(payloadFile).Decode(&payload); err != nil {
					log.Printf("[bulk] failed to decode payload: %v", err)
				} else {
					amcs = payload.AMC
					schemes = payload.Scheme
					dps = payload.DP
					demats = payload.Demat
					folios = payload.Folio
					log.Printf("[bulk] parsed payload file: amc=%d scheme=%d dp=%d demat=%d folio=%d",
						len(amcs), len(schemes), len(dps), len(demats), len(folios))
				}
				payloadFile.Close()
			}
		} else {
			// fallback: old style form fields
			mf := r.MultipartForm
			log.Printf("[bulk] multipart form values: %+v", mf.Value)

			// Debug: Print raw form values for each field
			if amcRaw, exists := mf.Value["amc"]; exists && len(amcRaw) > 0 {
				log.Printf("[bulk] raw amc data: %s", amcRaw[0])
			}
			if schemeRaw, exists := mf.Value["scheme"]; exists && len(schemeRaw) > 0 {
				log.Printf("[bulk] raw scheme data: %s", schemeRaw[0])
			}

			var err error
			amcs, err = parseJSONField[AMCInput](mf, "amc")
			if err != nil {
				log.Printf("[bulk] parse amc failed: %v", err)
			}
			schemes, err = parseJSONField[SchemeInput](mf, "scheme")
			if err != nil {
				log.Printf("[bulk] parse scheme failed: %v", err)
			}
			dps, err = parseJSONField[DPInput](mf, "dp")
			if err != nil {
				log.Printf("[bulk] parse dp failed: %v", err)
			}
			demats, err = parseJSONField[DematInput](mf, "demat")
			if err != nil {
				log.Printf("[bulk] parse demat failed: %v", err)
			}
			folios, err = parseJSONField[FolioInput](mf, "folio")
			if err != nil {
				log.Printf("[bulk] parse folio failed: %v", err)
			}
			log.Printf("[bulk] parsed arrays (legacy form): amc=%d scheme=%d dp=%d demat=%d folio=%d",
				len(amcs), len(schemes), len(dps), len(demats), len(folios))
		}

		// maps to hold created or existing ids
		amcMap := map[string]string{}    // key: internal_amc_code or amc_name -> amc_id
		schemeMap := map[string]string{} // key: internal_scheme_code -> scheme_id
		dpMap := map[string]string{}     // key: dp_code -> dp_id
		dematMap := map[string]string{}  // key: demat_account_number -> demat_id
		folioMap := map[string]string{}  // key: folio_number -> folio_id

<<<<<<< HEAD
		counts := map[string]int64{"amc": 0, "scheme": 0, "dp": 0, "demat": 0, "folio": 0, "transactions": 0, "snapshot": 0}
=======
		// Track which entities are enriched (pre-existing)
		enrichedAMCs := make(map[string]bool)
		enrichedSchemes := make(map[string]bool)
		enrichedDPs := make(map[string]bool)
		enrichedDemats := make(map[string]bool)
		enrichedFolios := make(map[string]bool)

		counts := map[string]int64{"amc": 0, "scheme": 0, "dp": 0, "demat": 0, "folio": 0, "transactions": 0, "snapshot": 0}
		enrichedCounts := map[string]int64{"amc": 0, "scheme": 0, "dp": 0, "demat": 0, "folio": 0}
>>>>>>> origin/main

		// Process AMCs
		log.Printf("[bulk] starting AMC processing, found %d AMCs", len(amcs))
		for i, a := range amcs {
			log.Printf("[bulk] processing amc[%d]: %+v", i, a)
			// lookup by internal code or name
			var existing string
			if a.InternalAmcCode != "" {
				log.Printf("[bulk] amc[%d] checking existing by internal_code: %s", i, a.InternalAmcCode)
				_ = tx.QueryRow(ctx, `SELECT amc_id FROM investment.masteramc WHERE internal_amc_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, a.InternalAmcCode).Scan(&existing)
			}
			if existing == "" && a.AmcName != "" {
				log.Printf("[bulk] amc[%d] checking existing by name: %s", i, a.AmcName)
				_ = tx.QueryRow(ctx, `SELECT amc_id FROM investment.masteramc WHERE amc_name=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, a.AmcName).Scan(&existing)
			}
			if existing != "" {
				log.Printf("[bulk] amc[%d] found existing: %s", i, existing)
<<<<<<< HEAD
				amcMap[defaultIfEmpty(a.InternalAmcCode, a.AmcName)] = existing
=======
				key := defaultIfEmpty(a.InternalAmcCode, a.AmcName)
				amcMap[key] = existing
>>>>>>> origin/main
				// Create mapping entry instead of updating batch_id
				_, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, amc_id) VALUES ($1, $2)`, batchID, existing)
				if err != nil {
					log.Printf("[bulk] amc[%d] mapping insert failed: %v", i, err)
					api.RespondWithError(w, 500, "amc mapping failed: "+err.Error())
					return
				}
				log.Printf("[bulk] amc[%d] created mapping for existing entity", i)
				// If enriched, don't create audit record
				if a.Enriched {
<<<<<<< HEAD
=======
					enrichedCounts["amc"]++
					enrichedAMCs[key] = true
>>>>>>> origin/main
					continue
				}
				// Otherwise, treat as duplicate
				continue
			}
			// insert
			var amcID string
			q := `INSERT INTO investment.masteramc (amc_name, internal_amc_code, status, primary_contact_name, primary_contact_email, sebi_registration_no, amc_beneficiary_name, amc_bank_account_no, amc_bank_name, amc_bank_ifsc, mfu_amc_code, cams_amc_code, erp_vendor_code, source, batch_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'Manual',$14) RETURNING amc_id`
			if err := tx.QueryRow(ctx, q,
				a.AmcName,
				a.InternalAmcCode,
				defaultIfEmpty(a.Status, "Active"),
				nullableString(a.PrimaryContactName),
				nullableString(a.PrimaryContactEmail),
				nullableString(a.SebiRegistrationNo),
				defaultIfEmpty(a.AmcBeneficiaryName, "Default Beneficiary"),
				defaultIfEmpty(a.AmcBankAccountNo, "000000000000"),
				defaultIfEmpty(a.AmcBankName, "Default Bank"),
				defaultIfEmpty(a.AmcBankIfsc, "DEFAULT0000"),
				defaultIfEmpty(a.MfuAmcCode, "MFU000"),
				defaultIfEmpty(a.CamsAmcCode, "CAM000"),
				defaultIfEmpty(a.ErpVendorCode, "ERP000"),
				batchID).Scan(&amcID); err != nil {
				log.Printf("[bulk] insert amc failed: %v", err)
				api.RespondWithError(w, 500, "insert amc failed: "+err.Error())
				return
			}
			log.Printf("[bulk] inserted amc[%d] %s (%s) with batch_id %s", i, amcID, a.AmcName, batchID)
			// Create mapping entry
			if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, amc_id) VALUES ($1, $2)`, batchID, amcID); err != nil {
				log.Printf("[bulk] amc[%d] mapping insert failed: %v", i, err)
				api.RespondWithError(w, 500, "amc mapping failed: "+err.Error())
				return
			}
			// audit
			if _, err := tx.Exec(ctx, `INSERT INTO investment.auditactionamc (amc_id, actiontype, processing_status, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, amcID, userEmail); err != nil {
				log.Printf("[bulk] audit amc[%d] insert failed: %v", i, err)
				api.RespondWithError(w, 500, "audit amc failed: "+err.Error())
				return
			}
			log.Printf("[bulk] created audit for amc[%d]: %s", i, amcID)
			amcMap[defaultIfEmpty(a.InternalAmcCode, a.AmcName)] = amcID
			counts["amc"]++
			log.Printf("[bulk] amc[%d] completed, count now: %d", i, counts["amc"])
		}
		log.Printf("[bulk] AMC processing completed. Total processed: %d, Total created: %d", len(amcs), counts["amc"])

		// Process DP
		for _, d := range dps {
			log.Printf("[bulk] processing dp: %+v", d)
			var existing string
			if d.DPCode != "" {
				_ = tx.QueryRow(ctx, `SELECT dp_id FROM investment.masterdepositoryparticipant WHERE dp_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, d.DPCode).Scan(&existing)
			}
			if existing != "" {
				dpMap[d.DPCode] = existing
				// Create mapping entry
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name) VALUES ($1, $2)`, batchID, d.DPName); err != nil {
					log.Printf("[bulk] dp mapping insert failed: %v", err)
					api.RespondWithError(w, 500, "dp mapping failed: "+err.Error())
					return
				}
				if d.Enriched {
<<<<<<< HEAD
					continue
=======
					enrichedCounts["dp"]++
					continue
					enrichedDPs[d.DPCode] = true
>>>>>>> origin/main
				}
				continue
			}
			var dpID string
			if err := tx.QueryRow(ctx, `INSERT INTO investment.masterdepositoryparticipant (dp_name, dp_code, depository, status, source, batch_id) VALUES ($1,$2,$3,$4,'Manual',$5) RETURNING dp_id`, d.DPName, d.DPCode, defaultIfEmpty(d.Depository, "NSDL"), defaultIfEmpty(d.Status, "Active"), batchID).Scan(&dpID); err != nil {
				log.Printf("[bulk] insert dp failed: %v", err)
				api.RespondWithError(w, 500, "insert dp failed: "+err.Error())
				return
			}
			// Create mapping entry
			if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name) VALUES ($1, $2)`, batchID, d.DPName); err != nil {
				log.Printf("[bulk] dp mapping insert failed: %v", err)
				api.RespondWithError(w, 500, "dp mapping failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `INSERT INTO investment.auditactiondp (dp_id, actiontype, processing_status, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, dpID, userEmail); err != nil {
				log.Printf("[bulk] audit dp failed: %v", err)
				api.RespondWithError(w, 500, "audit dp failed: "+err.Error())
				return
			}
			dpMap[d.DPCode] = dpID
			counts["dp"]++
		}

		// Process Demat
		for _, dm := range demats {
			log.Printf("[bulk] processing demat: %+v", dm)
			var existing string
			if dm.DematAccountNumber != "" {
				_ = tx.QueryRow(ctx, `SELECT demat_id FROM investment.masterdemataccount WHERE demat_account_number=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, dm.DematAccountNumber).Scan(&existing)
			}
			if existing != "" {
				dematMap[dm.DematAccountNumber] = existing
				// Create mapping entry
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, demat_id) VALUES ($1, $2, $3)`, batchID, dm.EntityName, existing); err != nil {
					log.Printf("[bulk] demat mapping insert failed: %v", err)
					api.RespondWithError(w, 500, "demat mapping failed: "+err.Error())
					return
				}
				if dm.Enriched {
<<<<<<< HEAD
					continue
				}
=======
					enrichedCounts["demat"]++
					continue
				}
				enrichedDemats[dm.DematAccountNumber] = true
>>>>>>> origin/main
				continue
			}
			var dematID string
			if err := tx.QueryRow(ctx, `INSERT INTO investment.masterdemataccount (entity_name, dp_id, depository, demat_account_number, depository_participant, client_id, default_settlement_account, status, source, batch_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'Manual',$9) RETURNING demat_id`, dm.EntityName, dm.DPID, defaultIfEmpty(dm.Depository, "NSDL"), dm.DematAccountNumber, dm.DepositoryParticipant, dm.ClientID, defaultIfEmpty(dm.DefaultSettlementAccount, "SYSTEM"), defaultIfEmpty(dm.Status, "Active"), batchID).Scan(&dematID); err != nil {
				log.Printf("[bulk] insert demat failed: %v", err)
				api.RespondWithError(w, 500, "insert demat failed: "+err.Error())
				return
			}
			// Create mapping entry
			if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, demat_id) VALUES ($1, $2, $3)`, batchID, dm.EntityName, dematID); err != nil {
				log.Printf("[bulk] demat mapping insert failed: %v", err)
				api.RespondWithError(w, 500, "demat mapping failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, dematID, userEmail); err != nil {
				log.Printf("[bulk] audit demat failed: %v", err)
				api.RespondWithError(w, 500, "audit demat failed: "+err.Error())
				return
			}
			dematMap[dm.DematAccountNumber] = dematID
			counts["demat"]++
		}

		// Process Folios
		for _, f := range folios {
			log.Printf("[bulk] processing folio: %+v", f)
			if f.Enriched && f.FolioID != "" {
				log.Printf("[bulk] folio is enriched with ID, using: %s", f.FolioID)
				folioMap[f.FolioNumber] = f.FolioID
				// Create mapping entry
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, folio_id, folio_number) VALUES ($1, $2, $3, $4)`, batchID, f.EntityName, f.FolioID, f.FolioNumber); err != nil {
<<<<<<< HEAD
					log.Printf(constants.ErrBulkFolioMappingInsertFailed, err)
					api.RespondWithError(w, 500, constants.ErrBulkFolioMappingFailed+err.Error())
					return
				}
				continue
=======
					log.Printf("[bulk] folio mapping insert failed: %v", err)
					api.RespondWithError(w, 500, "folio mapping failed: "+err.Error())
					return
				}
				enrichedCounts["scheme"]++
				continue
				enrichedFolios[f.FolioNumber] = true
>>>>>>> origin/main
			}
			// Resolve amc_name if empty - try to lookup from entity_name or use a default
			folioAmcName := f.AmcName
			if folioAmcName == "" {
				// Try to find an AMC for this entity or use first available
				for amcKey, _ := range amcMap {
					folioAmcName = amcKey
					break
				}
				if folioAmcName == "" {
					folioAmcName = "Unknown AMC"
				}
				log.Printf("[bulk] folio amc_name was empty, using: %s", folioAmcName)
			}
			var existing string
			if f.FolioNumber != "" && f.EntityName != "" {
				log.Printf("[bulk] folio checking existing: folio=%s, entity=%s, amc=%s", f.FolioNumber, f.EntityName, folioAmcName)
				_ = tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number=$1 AND entity_name=$2 AND COALESCE(is_deleted,false)=false LIMIT 1`, f.FolioNumber, f.EntityName).Scan(&existing)
			}
			if existing != "" {
				log.Printf("[bulk] folio found existing: %s", existing)
				folioMap[f.FolioNumber] = existing
				// Create mapping entry
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, folio_id, folio_number) VALUES ($1, $2, $3, $4)`, batchID, f.EntityName, existing, f.FolioNumber); err != nil {
<<<<<<< HEAD
					log.Printf(constants.ErrBulkFolioMappingInsertFailed, err)
					api.RespondWithError(w, 500, constants.ErrBulkFolioMappingFailed+err.Error())
=======
					log.Printf("[bulk] folio mapping insert failed: %v", err)
					api.RespondWithError(w, 500, "folio mapping failed: "+err.Error())
>>>>>>> origin/main
					return
				}
				if f.Enriched {
					continue
<<<<<<< HEAD
=======
					enrichedCounts["folio"]++
					enrichedFolios[f.FolioNumber] = true
>>>>>>> origin/main
				}
				continue
			}
			var folioID string
			if err := tx.QueryRow(ctx, `INSERT INTO investment.masterfolio (entity_name, amc_name, folio_number, first_holder_name, default_subscription_account, default_redemption_account, status, source, batch_id) VALUES ($1,$2,$3,$4,$5,$6,$7,'Manual',$8) RETURNING folio_id`, f.EntityName, folioAmcName, f.FolioNumber, f.FirstHolderName, defaultIfEmpty(f.DefaultSubscriptionAccount, "SYSTEM"), defaultIfEmpty(f.DefaultRedemptionAccount, "SYSTEM"), defaultIfEmpty(f.Status, "Active"), batchID).Scan(&folioID); err != nil {
				log.Printf("[bulk] insert folio failed: %v", err)
				api.RespondWithError(w, 500, "insert folio failed: "+err.Error())
				return
			}
			// Create mapping entry for new folio
			if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, folio_id, folio_number) VALUES ($1, $2, $3, $4)`, batchID, f.EntityName, folioID, f.FolioNumber); err != nil {
<<<<<<< HEAD
				log.Printf(constants.ErrBulkFolioMappingInsertFailed, err)
				api.RespondWithError(w, 500, constants.ErrBulkFolioMappingFailed+err.Error())
=======
				log.Printf("[bulk] folio mapping insert failed: %v", err)
				api.RespondWithError(w, 500, "folio mapping failed: "+err.Error())
>>>>>>> origin/main
				return
			}
			if _, err := tx.Exec(ctx, `INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, folioID, userEmail); err != nil {
				log.Printf("[bulk] audit folio failed: %v", err)
				api.RespondWithError(w, 500, "audit folio failed: "+err.Error())
				return
			}
			// map schemes if provided
			if len(f.SchemeRefs) > 0 {
				for _, sref := range f.SchemeRefs {
					// try to resolve schemeID by id/name/isin/internal_code
					var sid string
					_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE (scheme_id=$1 OR scheme_name=$1 OR isin=$1 OR internal_scheme_code=$1) AND COALESCE(is_deleted,false)=false LIMIT 1`, sref).Scan(&sid)
					if sid != "" {
						if _, err := tx.Exec(ctx, `INSERT INTO investment.folioschememapping (folio_id, scheme_id, status) VALUES ($1,$2,'Active') ON CONFLICT DO NOTHING`, folioID, sid); err != nil {
							log.Printf("[bulk] folio-scheme map failed: %v", err)
							api.RespondWithError(w, 500, "folio-scheme map failed: "+err.Error())
							return
						}
					}
				}
			}
			folioMap[f.FolioNumber] = folioID
			counts["folio"]++
		}

		// Process Schemes (after AMCs so amc exists)
		for _, s := range schemes {
			log.Printf("[bulk] processing scheme: %+v", s)
			var existing string

			// For enriched entities, look up by name and amc first
			if s.Enriched && s.SchemeName != "" {
				log.Printf("[bulk] scheme is enriched, looking up by name: %s (amc: %s)", s.SchemeName, s.AmcName)
				if s.AmcName != "" {
					_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE scheme_name=$1 AND amc_name=$2 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.SchemeName, s.AmcName).Scan(&existing)
				} else {
					_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE scheme_name=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.SchemeName).Scan(&existing)
				}
				if existing != "" {
					log.Printf("[bulk] scheme found enriched entity: %s", existing)
					schemeMap[defaultIfEmpty(s.InternalSchemeCode, s.SchemeName)] = existing
					// Create mapping entry for enriched scheme
					if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, scheme_id, scheme_name) VALUES ($1, $2, $3)`, batchID, existing, s.SchemeName); err != nil {
<<<<<<< HEAD
						log.Printf(constants.ErrBulkSchemeMappingInsertFailed, err)
						api.RespondWithError(w, 500, constants.ErrBulkSchemeMappingFailed+err.Error())
=======
						log.Printf("[bulk] scheme mapping insert failed: %v", err)
						api.RespondWithError(w, 500, "scheme mapping failed: "+err.Error())
>>>>>>> origin/main
						return
					}
					continue
				}
			}

			// lookup by internal code, isin, or name
			if s.InternalSchemeCode != "" {
				log.Printf("[bulk] scheme checking existing by internal_code: %s", s.InternalSchemeCode)
				_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE internal_scheme_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.InternalSchemeCode).Scan(&existing)
<<<<<<< HEAD
=======
				enrichedSchemes[defaultIfEmpty(s.InternalSchemeCode, s.SchemeName)] = true
>>>>>>> origin/main
			}
			if existing == "" && s.ISIN != "" {
				log.Printf("[bulk] scheme checking existing by isin: %s", s.ISIN)
				_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE isin=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.ISIN).Scan(&existing)
			}
			if existing == "" && s.SchemeName != "" {
				log.Printf("[bulk] scheme checking existing by name: %s", s.SchemeName)
				_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE scheme_name=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.SchemeName).Scan(&existing)
			}
			if existing != "" {
				log.Printf("[bulk] scheme found existing: %s", existing)
				schemeMap[defaultIfEmpty(s.InternalSchemeCode, s.SchemeName)] = existing
				// Create mapping entry for existing scheme
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, scheme_id, scheme_name) VALUES ($1, $2, $3)`, batchID, existing, s.SchemeName); err != nil {
<<<<<<< HEAD
					log.Printf(constants.ErrBulkSchemeMappingInsertFailed, err)
					api.RespondWithError(w, 500, constants.ErrBulkSchemeMappingFailed+err.Error())
					return
				}
				if s.Enriched {
=======
					log.Printf("[bulk] scheme mapping insert failed: %v", err)
					api.RespondWithError(w, 500, "scheme mapping failed: "+err.Error())
					return
				}
				if s.Enriched {
					enrichedCounts["scheme"]++
>>>>>>> origin/main
					continue
				}
				continue
			}
			var schemeID string
<<<<<<< HEAD
=======
			enrichedSchemes[defaultIfEmpty(s.InternalSchemeCode, s.SchemeName)] = true
>>>>>>> origin/main
			if err := tx.QueryRow(ctx, `INSERT INTO investment.masterscheme (scheme_name, isin, amc_name, internal_scheme_code, internal_risk_rating, erp_gl_account, status, source, batch_id) VALUES ($1,$2,$3,$4,$5,$6,$7,'Manual',$8) RETURNING scheme_id`,
				s.SchemeName,
				s.ISIN,
				s.AmcName,
				s.InternalSchemeCode,
				defaultIfEmpty(s.InternalRiskRating, "Medium"),
				defaultIfEmpty(s.ErpGlAccount, "GL000"),
				defaultIfEmpty(s.Status, "Active"),
				batchID).Scan(&schemeID); err != nil {
				log.Printf("[bulk] insert scheme failed: %v", err)
				api.RespondWithError(w, 500, "insert scheme failed: "+err.Error())
				return
			}
			// Create mapping entry for new scheme
			if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, scheme_id, scheme_name) VALUES ($1, $2, $3)`, batchID, schemeID, s.SchemeName); err != nil {
<<<<<<< HEAD
				log.Printf(constants.ErrBulkSchemeMappingInsertFailed, err)
				api.RespondWithError(w, 500, constants.ErrBulkSchemeMappingFailed+err.Error())
=======
				log.Printf("[bulk] scheme mapping insert failed: %v", err)
				api.RespondWithError(w, 500, "scheme mapping failed: "+err.Error())
>>>>>>> origin/main
				return
			}
			if _, err := tx.Exec(ctx, `INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, schemeID, userEmail); err != nil {
				log.Printf("[bulk] audit scheme failed: %v", err)
				api.RespondWithError(w, 500, "audit scheme failed: "+err.Error())
				return
			}
			schemeMap[s.InternalSchemeCode] = schemeID
			counts["scheme"]++
		}

		// Insert onboard_mapping for all created or enriched entries
		for k, v := range amcMap {
<<<<<<< HEAD
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'AMC',$3,$4)`, batchID, v, k, true); err != nil {
				log.Printf("[bulk] onboard mapping amc failed: %v", err)
				api.RespondWithError(w, 500, constants.ErrBulkOnboardMappingFailed+err.Error())
=======
			isEnriched := enrichedAMCs[k]
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'AMC',$3,$4)`, batchID, v, k, isEnriched); err != nil {
				log.Printf("[bulk] onboard mapping amc failed: %v", err)
				api.RespondWithError(w, 500, "onboard mapping failed: "+err.Error())
>>>>>>> origin/main
				return
			}
		}
		for k, v := range schemeMap {
<<<<<<< HEAD
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'SCHEME',$3,$4)`, batchID, v, k, true); err != nil {
				log.Printf("[bulk] onboard mapping scheme failed: %v", err)
				api.RespondWithError(w, 500, constants.ErrBulkOnboardMappingFailed+err.Error())
=======
			isEnriched := enrichedSchemes[k]
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'SCHEME',$3,$4)`, batchID, v, k, isEnriched); err != nil {
				log.Printf("[bulk] onboard mapping scheme failed: %v", err)
				api.RespondWithError(w, 500, "onboard mapping failed: "+err.Error())
>>>>>>> origin/main
				return
			}
		}
		for k, v := range dpMap {
<<<<<<< HEAD
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'DP',$3,$4)`, batchID, v, k, true); err != nil {
				log.Printf("[bulk] onboard mapping dp failed: %v", err)
				api.RespondWithError(w, 500, constants.ErrBulkOnboardMappingFailed+err.Error())
=======
			isEnriched := enrichedDPs[k]
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'DP',$3,$4)`, batchID, v, k, isEnriched); err != nil {
				log.Printf("[bulk] onboard mapping dp failed: %v", err)
				api.RespondWithError(w, 500, "onboard mapping failed: "+err.Error())
>>>>>>> origin/main
				return
			}
		}
		for k, v := range dematMap {
<<<<<<< HEAD
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'DEMAT',$3,$4)`, batchID, v, k, true); err != nil {
				log.Printf("[bulk] onboard mapping demat failed: %v", err)
				api.RespondWithError(w, 500, constants.ErrBulkOnboardMappingFailed+err.Error())
=======
			isEnriched := enrichedDemats[k]
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'DEMAT',$3,$4)`, batchID, v, k, isEnriched); err != nil {
				log.Printf("[bulk] onboard mapping demat failed: %v", err)
				api.RespondWithError(w, 500, "onboard mapping failed: "+err.Error())
>>>>>>> origin/main
				return
			}
		}
		for k, v := range folioMap {
<<<<<<< HEAD
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'FOLIO',$3,$4)`, batchID, v, k, true); err != nil {
				log.Printf("[bulk] onboard mapping folio failed: %v", err)
				api.RespondWithError(w, 500, constants.ErrBulkOnboardMappingFailed+err.Error())
=======
			isEnriched := enrichedFolios[k]
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'FOLIO',$3,$4)`, batchID, v, k, isEnriched); err != nil {
				log.Printf("[bulk] onboard mapping folio failed: %v", err)
				api.RespondWithError(w, 500, "onboard mapping failed: "+err.Error())
>>>>>>> origin/main
				return
			}
		}
		log.Printf("[bulk] onboard_mapping inserted")

		// Insert portfolio_onboarding_map rows for all entities
		log.Printf("[bulk] populating portfolio_onboarding_map for all entities")

		// Insert for AMCs
		for amcKey, amcID := range amcMap {
			var entityName, amcName sql.NullString
			if err := tx.QueryRow(ctx, `SELECT amc_name, amc_name FROM investment.masteramc WHERE amc_id=$1`, amcID).Scan(&entityName, &amcName); err == nil {
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, amc_id, created_at) VALUES ($1,$2,$3,now())`, batchID, entityName.String, amcID); err != nil {
					log.Printf("[bulk] portfolio_onboarding_map AMC insert failed: %v", err)
				} else {
					log.Printf("[bulk] inserted portfolio_onboarding_map for AMC: %s", amcKey)
				}
			}
		}

		// Insert for DPs
		for dpKey, dpID := range dpMap {
			var entityName, dpName sql.NullString
			if err := tx.QueryRow(ctx, `SELECT dp_name, dp_name FROM investment.masterdepositoryparticipant WHERE dp_id=$1`, dpID).Scan(&entityName, &dpName); err == nil {
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, created_at) VALUES ($1,$2,now())`, batchID, entityName.String); err != nil {
					log.Printf("[bulk] portfolio_onboarding_map DP insert failed: %v", err)
				} else {
					log.Printf("[bulk] inserted portfolio_onboarding_map for DP: %s", dpKey)
				}
			}
		}

		// Insert for Demats
		for dematKey, dematID := range dematMap {
			var entityName, dematAccNum sql.NullString
			if err := tx.QueryRow(ctx, `SELECT entity_name, demat_account_number FROM investment.masterdemataccount WHERE demat_id=$1`, dematID).Scan(&entityName, &dematAccNum); err == nil {
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, demat_id, created_at) VALUES ($1,$2,$3,now())`, batchID, entityName.String, dematID); err != nil {
					log.Printf("[bulk] portfolio_onboarding_map Demat insert failed: %v", err)
				} else {
					log.Printf("[bulk] inserted portfolio_onboarding_map for Demat: %s", dematKey)
				}
			}
		}

		// Insert for Schemes
		for schemeKey, schemeID := range schemeMap {
			var entityName, schemeName sql.NullString
			if err := tx.QueryRow(ctx, `SELECT amc_name, scheme_name FROM investment.masterscheme WHERE scheme_id=$1`, schemeID).Scan(&entityName, &schemeName); err == nil {
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, scheme_id, scheme_name, created_at) VALUES ($1,$2,$3,$4,now())`, batchID, entityName.String, schemeID, schemeName.String); err != nil {
					log.Printf("[bulk] portfolio_onboarding_map Scheme insert failed: %v", err)
				} else {
					log.Printf("[bulk] inserted portfolio_onboarding_map for Scheme: %s", schemeKey)
				}
			}
		}

		// Insert for Folios (enhanced with related data)
		for folioKey, folioID := range folioMap {
			var entityName, folioNumber, amcNameVal sql.NullString
			if err := tx.QueryRow(ctx, `SELECT entity_name, folio_number, amc_name FROM investment.masterfolio WHERE folio_id=$1`, folioID).Scan(&entityName, &folioNumber, &amcNameVal); err == nil {
				// Get AMC ID if possible
				var relatedAmcID sql.NullString
				if amcNameVal.Valid {
					_ = tx.QueryRow(ctx, `SELECT amc_id FROM investment.masteramc WHERE amc_name=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, amcNameVal.String).Scan(&relatedAmcID)
				}

				// Get related schemes for this folio
				schemeRows, _ := tx.Query(ctx, `SELECT s.scheme_id, s.scheme_name FROM investment.folioschememapping fsm JOIN investment.masterscheme s ON s.scheme_id = fsm.scheme_id WHERE fsm.folio_id=$1`, folioID)
				schemeFound := false
				for schemeRows.Next() {
					var sid, sname string
					_ = schemeRows.Scan(&sid, &sname)
					// Insert with scheme details
					if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, folio_id, folio_number, amc_id, scheme_id, scheme_name, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,now())`,
						batchID, entityName.String, folioID, folioNumber.String,
						nullableString(relatedAmcID.String), sid, sname); err != nil {
						log.Printf("[bulk] portfolio_onboarding_map Folio+Scheme insert failed: %v", err)
					} else {
						log.Printf("[bulk] inserted portfolio_onboarding_map for Folio+Scheme: %s+%s", folioKey, sname)
						schemeFound = true
					}
				}
				schemeRows.Close()

				// If no schemes found, insert folio-only record
				if !schemeFound {
					if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, folio_id, folio_number, amc_id, created_at) VALUES ($1,$2,$3,$4,$5,now())`,
						batchID, entityName.String, folioID, folioNumber.String, nullableString(relatedAmcID.String)); err != nil {
						log.Printf("[bulk] portfolio_onboarding_map Folio insert failed: %v", err)
					} else {
						log.Printf("[bulk] inserted portfolio_onboarding_map for Folio: %s", folioKey)
					}
				}
			}
		}
		log.Printf("[bulk] portfolio_onboarding_map populated for all entity types")

<<<<<<< HEAD
		// Parse transactions CSV if present
		var txRows []TxCSVRow
		if fhArr := r.MultipartForm.File["transactions"]; len(fhArr) > 0 {
=======
		// Parse transactions CSV if present - check both "files" and "transactions" keys
		var txRows []TxCSVRow
		var fhArr []*multipart.FileHeader
		if files := r.MultipartForm.File["files"]; len(files) > 0 {
			fhArr = files
			log.Printf("[bulk] found CSV under 'files' key")
		} else if txFiles := r.MultipartForm.File["transactions"]; len(txFiles) > 0 {
			fhArr = txFiles
			log.Printf("[bulk] found CSV under 'transactions' key")
		}
		
		if len(fhArr) > 0 {
>>>>>>> origin/main
			f, err := fhArr[0].Open()
			if err != nil {
				log.Printf("[bulk] open transactions file: %v", err)
				api.RespondWithError(w, 400, "open transactions file: "+err.Error())
				return
			}
			txRows, err = parseTransactionsCSV(f)
			f.Close()
			if err != nil {
				log.Printf("[bulk] parse transactions csv: %v", err)
				api.RespondWithError(w, 400, "parse transactions: "+err.Error())
				return
			}
			log.Printf("[bulk] parsed %d transaction rows", len(txRows))
<<<<<<< HEAD
			// insert transactions -- use prepared multi-row insert in this tx
			for _, tr := range txRows {
				// resolve scheme_id
				var schemeID string
				if v, ok := schemeMap[tr.SchemeInternalCode]; ok {
					schemeID = v
				} else {
					_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE internal_scheme_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, tr.SchemeInternalCode).Scan(&schemeID)
				}
				// resolve folio_id if folio_number provided
=======

			// Build reverse lookup maps for enriched data validation
			enrichedSchemesByCode := make(map[string]SchemeInput)
			for _, s := range schemes {
				if s.InternalSchemeCode != "" {
					enrichedSchemesByCode[s.InternalSchemeCode] = s
				}
			}
			enrichedFoliosByNumber := make(map[string]FolioInput)
			for _, f := range folios {
				if f.FolioNumber != "" {
					enrichedFoliosByNumber[f.FolioNumber] = f
				}
			}
			enrichedDematsByNumber := make(map[string]DematInput)
			for _, d := range demats {
				if d.DematAccountNumber != "" {
					enrichedDematsByNumber[d.DematAccountNumber] = d
				}
			}

			// Insert transactions with validation and enrichment
			for _, tr := range txRows {
				// Validate transaction against enriched data if provided
				if enrichedScheme, exists := enrichedSchemesByCode[tr.SchemeInternalCode]; exists {
					log.Printf("[bulk] transaction scheme %s validated against enriched scheme: %s", tr.SchemeInternalCode, enrichedScheme.SchemeName)
				}
				if tr.FolioNumber != "" {
					if enrichedFolio, exists := enrichedFoliosByNumber[tr.FolioNumber]; exists {
						log.Printf("[bulk] transaction folio %s validated against enriched folio for entity: %s", tr.FolioNumber, enrichedFolio.EntityName)
					}
				}
				if tr.DematAccNumber != "" {
					if enrichedDemat, exists := enrichedDematsByNumber[tr.DematAccNumber]; exists {
						log.Printf("[bulk] transaction demat %s validated against enriched demat for entity: %s", tr.DematAccNumber, enrichedDemat.EntityName)
					}
				}

				// Resolve entity_name from folio or demat
				var entityName string
				if tr.FolioNumber != "" {
					// Try from folios map first (enriched or newly created)
					for _, f := range folios {
						if f.FolioNumber == tr.FolioNumber {
							entityName = f.EntityName
							break
						}
					}
					// If not found, query DB
					if entityName == "" {
						_ = tx.QueryRow(ctx, `SELECT entity_name FROM investment.masterfolio WHERE folio_number=$1 AND COALESCE(is_deleted,false)=false`, tr.FolioNumber).Scan(&entityName)
					}
				} else if tr.DematAccNumber != "" {
					// Try from demats map first
					for _, d := range demats {
						if d.DematAccountNumber == tr.DematAccNumber {
							entityName = d.EntityName
							break
						}
					}
					// If not found, query DB
					if entityName == "" {
						_ = tx.QueryRow(ctx, `SELECT entity_name FROM investment.masterdemataccount WHERE demat_account_number=$1 AND COALESCE(is_deleted,false)=false`, tr.DematAccNumber).Scan(&entityName)
					}
				}

				// Resolve scheme_id (prioritize from enriched data, fallback to DB)
				var schemeID string
				if v, ok := schemeMap[tr.SchemeInternalCode]; ok {
					schemeID = v
					log.Printf("[bulk] transaction resolved scheme_id from schemeMap: %s -> %s", tr.SchemeInternalCode, schemeID)
				} else {
					_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE internal_scheme_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, tr.SchemeInternalCode).Scan(&schemeID)
					if schemeID != "" {
						log.Printf("[bulk] transaction resolved scheme_id from DB: %s -> %s", tr.SchemeInternalCode, schemeID)
					} else {
						log.Printf("[bulk] WARNING: transaction could not resolve scheme_id for internal_code: %s", tr.SchemeInternalCode)
					}
				}

				// Resolve folio_id if folio_number provided (prioritize from enriched data, fallback to DB)
>>>>>>> origin/main
				var folioID string
				if tr.FolioNumber != "" {
					if v, ok := folioMap[tr.FolioNumber]; ok {
						folioID = v
					} else {
						_ = tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, tr.FolioNumber).Scan(&folioID)
					}
				}
<<<<<<< HEAD
				// resolve demat_id if demat_acc_number provided
=======

				// Resolve demat_id if demat_acc_number provided (prioritize from enriched data, fallback to DB)
>>>>>>> origin/main
				var dematID string
				if tr.DematAccNumber != "" {
					if v, ok := dematMap[tr.DematAccNumber]; ok {
						dematID = v
					} else {
						_ = tx.QueryRow(ctx, `SELECT demat_id FROM investment.masterdemataccount WHERE demat_account_number=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, tr.DematAccNumber).Scan(&dematID)
					}
				}
<<<<<<< HEAD
				if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_transaction (batch_id, transaction_date, transaction_type, scheme_internal_code, folio_number, demat_acc_number, amount, units, nav, scheme_id, folio_id, demat_id, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now())`, batchID, tr.TransactionDate, tr.TransactionType, tr.SchemeInternalCode, nullableString(tr.FolioNumber), nullableString(tr.DematAccNumber), tr.Amount, tr.Units, tr.Nav, nullableString(schemeID), nullableString(folioID), nullableString(dematID)); err != nil {
=======

				// Insert transaction with resolved IDs and entity_name
				// Note: Store all values as POSITIVE. Transaction type ('Purchase', 'Sell', etc.) 
				// determines direction in snapshot calculations.
				if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_transaction (batch_id, transaction_date, transaction_type, scheme_internal_code, folio_number, demat_acc_number, amount, units, nav, scheme_id, folio_id, demat_id, entity_name, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now())`, batchID, tr.TransactionDate, tr.TransactionType, tr.SchemeInternalCode, nullableString(tr.FolioNumber), nullableString(tr.DematAccNumber), tr.Amount, tr.Units, tr.Nav, nullableString(schemeID), nullableString(folioID), nullableString(dematID), nullableString(entityName)); err != nil {
>>>>>>> origin/main
					log.Printf("[bulk] insert onboard_transaction failed: %v", err)
					api.RespondWithError(w, 500, "insert transaction failed: "+err.Error())
					return
				}
				counts["transactions"]++
			}
<<<<<<< HEAD
			log.Printf("[bulk] inserted %d transactions", counts["transactions"])
=======
			log.Printf("[bulk] inserted %d transactions with entity_name populated (validated against enriched data where available)", counts["transactions"])
			
			// STRICT VALIDATION: Ensure at least one transaction was inserted
			if counts["transactions"] == 0 {
				log.Printf("[bulk] STRICT: no transactions inserted, rolling back batch %s", batchID)
				api.RespondWithError(w, 400, "No transactions were inserted. Upload must contain valid transactions.")
				return
			}
>>>>>>> origin/main
		}

		// Only create snapshots if there are transactions
		if counts["transactions"] > 0 {
			aggQ := `
WITH scheme_resolved AS (
    SELECT
        ot.batch_id,
        ot.transaction_date,
        ot.transaction_type,
        ot.amount,
        ot.units,
        ot.nav,
<<<<<<< HEAD
        COALESCE(mf.entity_name, md.entity_name) AS entity_name,
        ot.folio_number,
        ot.demat_acc_number,
        COALESCE(fsm.scheme_id, ms2.scheme_id) AS scheme_id,
        COALESCE(ms.scheme_name, ms2.scheme_name) AS scheme_name,
        COALESCE(ms.isin, ms2.isin) AS isin
    FROM investment.onboard_transaction ot
    LEFT JOIN investment.masterfolio mf 
        ON mf.folio_number = ot.folio_number
    LEFT JOIN investment.masterdemataccount md
        ON md.demat_account_number = ot.demat_acc_number
    LEFT JOIN investment.folioschememapping fsm 
        ON fsm.folio_id = mf.folio_id
    LEFT JOIN investment.masterscheme ms 
        ON ms.scheme_id = fsm.scheme_id
    LEFT JOIN investment.masterscheme ms2
        ON ms2.internal_scheme_code = ot.scheme_internal_code
=======
        COALESCE(ot.entity_name, mf.entity_name, md.entity_name) AS entity_name,
        ot.folio_number,
        ot.demat_acc_number,
        ot.folio_id,
        ot.demat_id,
        -- Prioritize scheme_id from transaction, then resolve via internal_code or folio mapping
        COALESCE(ot.scheme_id, ms_direct.scheme_id, ms_code.scheme_id, ms_folio.scheme_id) AS scheme_id,
        COALESCE(ms_direct.scheme_name, ms_code.scheme_name, ms_folio.scheme_name) AS scheme_name,
        COALESCE(ms_direct.isin, ms_code.isin, ms_folio.isin) AS isin
    FROM investment.onboard_transaction ot
    LEFT JOIN investment.masterfolio mf 
        ON mf.folio_id = ot.folio_id
    LEFT JOIN investment.masterdemataccount md
        ON md.demat_id = ot.demat_id
    -- Direct scheme_id lookup
    LEFT JOIN investment.masterscheme ms_direct
        ON ms_direct.scheme_id = ot.scheme_id
    -- Lookup by internal_scheme_code
    LEFT JOIN investment.masterscheme ms_code
        ON ms_code.internal_scheme_code = ot.scheme_internal_code
    -- Lookup via folio scheme mapping
    LEFT JOIN investment.folioschememapping fsm 
        ON fsm.folio_id = ot.folio_id
    LEFT JOIN investment.masterscheme ms_folio
        ON ms_folio.scheme_id = fsm.scheme_id
>>>>>>> origin/main
    WHERE ot.batch_id = $1
),
transaction_summary AS (
    SELECT
        entity_name,
        folio_number,
        demat_acc_number,
<<<<<<< HEAD
=======
        folio_id,
        demat_id,
>>>>>>> origin/main
        scheme_id,
        scheme_name,
        isin,
        SUM(CASE 
            WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
            WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -units
            ELSE units
        END) AS total_units,
        SUM(CASE 
            WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN amount
            WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -amount
            ELSE amount
        END) AS total_invested_amount,
        CASE 
            WHEN SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
                WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -units
                ELSE units
            END) = 0 THEN 0
            ELSE SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN nav * units
                WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -nav * units
                ELSE nav * units
            END) / SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
                WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -units
                ELSE units
            END)
        END AS avg_nav
    FROM scheme_resolved
<<<<<<< HEAD
    GROUP BY entity_name, folio_number, demat_acc_number, scheme_id, scheme_name, isin
=======
    GROUP BY entity_name, folio_number, demat_acc_number, folio_id, demat_id, scheme_id, scheme_name, isin
>>>>>>> origin/main
),
latest_nav AS (
    SELECT DISTINCT ON (scheme_name)
        scheme_name,
        isin_div_payout_growth as isin,
        nav_value,
        nav_date
    FROM investment.amfi_nav_staging
    ORDER BY scheme_name, nav_date DESC
)
INSERT INTO investment.portfolio_snapshot (
  batch_id,
  entity_name,
  folio_number,
  demat_acc_number,
<<<<<<< HEAD
=======
  folio_id,
  demat_id,
>>>>>>> origin/main
  scheme_id,
  scheme_name,
  isin,
  total_units,
  avg_nav,
  current_nav,
  current_value,
  total_invested_amount,
  gain_loss,
  gain_losss_percent,
  created_at
)
SELECT
  $1 AS batch_id,
  ts.entity_name,
  ts.folio_number,
  ts.demat_acc_number,
<<<<<<< HEAD
=======
  ts.folio_id,
  ts.demat_id,
>>>>>>> origin/main
  ts.scheme_id,
  ts.scheme_name,
  ts.isin,
  ts.total_units,
  ts.avg_nav,
  COALESCE(ln.nav_value, 0) AS current_nav,
  ts.total_units * COALESCE(ln.nav_value, 0) AS current_value,
  ts.total_invested_amount,
  (ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount AS gain_loss,
  CASE 
    WHEN ts.total_invested_amount = 0 THEN 0
    ELSE (((ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount) / ts.total_invested_amount) * 100
  END AS gain_losss_percent,
  NOW()
FROM transaction_summary ts
LEFT JOIN latest_nav ln ON (ln.scheme_name = ts.scheme_name OR ln.isin = ts.isin)
WHERE ts.total_units > 0;
`

			if _, err := tx.Exec(ctx, aggQ, batchID); err != nil {
				log.Printf("[bulk] insert snapshot failed: %v", err)
				api.RespondWithError(w, 500, "snapshot insert failed: "+err.Error())
				return
			}
			// count snapshot rows
			var snapshotCnt int64
			if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM investment.portfolio_snapshot WHERE batch_id=$1`, batchID).Scan(&snapshotCnt); err != nil {
				log.Printf("[bulk] snapshot count failed: %v", err)
				api.RespondWithError(w, 500, "snapshot count failed: "+err.Error())
				return
			}
			counts["snapshot"] = snapshotCnt
			log.Printf("[bulk] inserted %d snapshot rows", counts["snapshot"])
		} else {
			log.Printf("[bulk] no transactions found, skipping snapshot creation")
		}

		// finalize batch update totals & status
		if _, err := tx.Exec(ctx, `UPDATE investment.onboard_batch SET total_records = (SELECT COUNT(*) FROM investment.onboard_transaction WHERE batch_id=$1), status='COMPLETED', completed_at=now() WHERE batch_id=$1`, batchID); err != nil {
			log.Printf("[bulk] finalize batch failed: %v", err)
			api.RespondWithError(w, 500, "finalize batch failed: "+err.Error())
			return
		}

		// commit
		if err := tx.Commit(ctx); err != nil {
			log.Printf("[bulk] commit failed: %v", err)
<<<<<<< HEAD
			api.RespondWithError(w, 500, constants.ErrCommitFailed+err.Error())
=======
			api.RespondWithError(w, 500, "commit failed: "+err.Error())
>>>>>>> origin/main
			return
		}
		tx = nil
		log.Printf("[bulk] committed batch %s", batchID)
		log.Printf("[bulk] final counts: %+v", counts)
<<<<<<< HEAD

		res := UploadResponse{Success: true, BatchID: batchID, Counts: counts, Message: "bulk onboard completed"}
=======
		log.Printf("[bulk] enriched counts: %+v", enrichedCounts)

		res := UploadResponse{Success: true, BatchID: batchID, Counts: counts, EnrichedCounts: enrichedCounts, Message: "bulk onboard completed"}
>>>>>>> origin/main
		log.Printf("[bulk] sending response: %+v", res)
		api.RespondWithPayload(w, true, "", res)
	}
}

// helper to return nil for empty strings for Exec params
func nullableString(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// NormalizeDate parses various date formats and returns YYYY-MM-DD format
func NormalizeDate(dateStr string) (string, error) {
	dateStr = strings.TrimSpace(dateStr)
	if dateStr == "" {
		return "", nil
	}

	// Normalize spaces
	dateStr = regexp.MustCompile(`\s+`).ReplaceAllString(dateStr, " ")

	// Try common layouts first
	layouts := []string{
		// ISO formats
<<<<<<< HEAD
		constants.DateFormat,
		"2006/01/02",
		"2006.01.02",
		time.RFC3339,
		constants.DateTimeFormat,
		constants.DateFormatISO,
=======
		"2006-01-02",
		"2006/01/02",
		"2006.01.02",
		time.RFC3339,
		constants.DateTimeFormat,
		"2006-01-02T15:04:05",
>>>>>>> origin/main
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",

		// DD-MM-YYYY formats
<<<<<<< HEAD
		constants.DateFormatAlt,
=======
		"02-01-2006",
>>>>>>> origin/main
		"02/01/2006",
		"02.01.2006",
		"02-01-2006 15:04:05",
		"02/01/2006 15:04:05",
		"02.01.2006 15:04:05",

		// MM-DD-YYYY formats
		"01-02-2006",
		"01/02/2006",
		"01.02.2006",
		"01-02-2006 15:04:05",
		"01/02/2006 15:04:05",
		"01.02.2006 15:04:05",

		// Text month formats
		"02-Jan-2006",
		"02-Jan-06",
		"2-Jan-2006",
		"2-Jan-06",
		"02-Jan-2006 15:04:05",
		"02 Jan 2006",
		"2 Jan 2006",
		"02 Jan 06",
		"2 Jan 06",
		"Jan 02, 2006",
		"Jan 2, 2006",
		"January 02, 2006",
		"January 2, 2006",

		// Single digit day/month formats
		"2-1-2006",
		"2/1/2006",
		"2.1.2006",
		"1-2-2006",
		"1/2/2006",
		"1.2.2006",

		// Short year formats
		"02-01-06",
		"02/01/06",
		"02.01.06",
		"01-02-06",
		"01/02/06",
		"01.02.06",
		"2-1-06",
		"2/1/06",
		"1-2-06",
		"1/2-06",

		// compact
		"20060102",
	}

	for _, l := range layouts {
		if t, err := time.Parse(l, dateStr); err == nil {
			if t.Year() < 1900 || t.Year() > 9999 {
				continue
			}
<<<<<<< HEAD
			return t.Format(constants.DateFormat), nil
=======
			return t.Format("2006-01-02"), nil
>>>>>>> origin/main
		}
	}

	// If the string is purely numeric try several heuristics
	digits := true
	for _, r := range dateStr {
		if r < '0' || r > '9' {
			digits = false
			break
		}
	}

	if digits {
		// YYYYMMDD
		if len(dateStr) == 8 {
			if y, err := strconv.Atoi(dateStr[0:4]); err == nil {
				if m, err := strconv.Atoi(dateStr[4:6]); err == nil {
					if d, err := strconv.Atoi(dateStr[6:8]); err == nil {
						if y >= 1900 && y <= 9999 {
<<<<<<< HEAD
							return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).Format(constants.DateFormat), nil
=======
							return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).Format("2006-01-02"), nil
>>>>>>> origin/main
						}
					}
				}
			}
		}

		if v, err := strconv.ParseInt(dateStr, 10, 64); err == nil {
			var t time.Time
			switch {
			case v >= 1e17:
				// nanoseconds since epoch
				t = time.Unix(0, v)
			case v >= 1e14:
				// microseconds -> ns
				t = time.Unix(0, v*1000)
			case v >= 1e11:
				// milliseconds -> ns
				t = time.Unix(0, v*1000000)
			case v >= 1e9:
				// seconds
				t = time.Unix(v, 0)
			default:
				// Treat as Excel serial date (days since 1899-12-30)
				base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
				t = base.AddDate(0, 0, int(v))
			}
			if t.Year() >= 1900 && t.Year() <= 9999 {
<<<<<<< HEAD
				return t.Format(constants.DateFormat), nil
=======
				return t.Format("2006-01-02"), nil
>>>>>>> origin/main
			}
		}
	}

	return "", fmt.Errorf("unparseable date: %s", dateStr)
}

// -------------------------
// Dashboard: Generate portfolio KPIs and holdings JSON
// -------------------------

type Holding struct {
	SchemeName   string  `json:"scheme_name"`
	Folio        string  `json:"folio"`
	TotalUnits   float64 `json:"total_units"`
	CurrentNav   float64 `json:"current_nav"`
	CurrentValue float64 `json:"current_value"`
}

type DashboardResponse struct {
	TotalInvestment float64            `json:"total_investment"`
	CurrentValue    float64            `json:"current_value"`
	GainLoss        float64            `json:"gain_loss"`
	GainLossPct     float64            `json:"gain_loss_pct"`
	Allocation      map[string]float64 `json:"allocation"`
	Holdings        []Holding          `json:"holdings"`
}

// RefreshPortfolioSnapshot recalculates portfolio snapshot with latest NAV data
func RefreshPortfolioSnapshot(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var req struct {
			BatchID string `json:"batch_id"`
		}

		// Support both JSON and form
<<<<<<< HEAD
		contentType := r.Header.Get(constants.ContentTypeText)
		if strings.Contains(contentType, constants.ContentTypeJSON) {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				api.RespondWithError(w, 400, constants.ErrInvalidJSONPrefix+err.Error())
=======
		contentType := r.Header.Get("Content-Type")
		if strings.Contains(contentType, "application/json") {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				api.RespondWithError(w, 400, "invalid JSON: "+err.Error())
>>>>>>> origin/main
				return
			}
		} else {
			_ = r.ParseForm()
			req.BatchID = r.FormValue("batch_id")
		}

		if req.BatchID == "" {
			api.RespondWithError(w, 400, "batch_id required")
			return
		}

		batchID := req.BatchID
		log.Printf("[refresh] refreshing portfolio snapshot for batch %s", batchID)

		// Delete existing snapshots for this batch
		if _, err := pgxPool.Exec(ctx, `DELETE FROM investment.portfolio_snapshot WHERE batch_id = $1`, batchID); err != nil {
			log.Printf("[refresh] delete existing snapshots failed: %v", err)
			api.RespondWithError(w, 500, "delete snapshots failed: "+err.Error())
			return
		}

		// Recalculate snapshots with current NAV data
		recalcQ := `
WITH scheme_resolved AS (
    SELECT
        ot.batch_id,
        ot.transaction_date,
        ot.transaction_type,
        ot.amount,
        ot.units,
        ot.nav,
        COALESCE(mf.entity_name, md.entity_name) AS entity_name,
        ot.folio_number,
        ot.demat_acc_number,
<<<<<<< HEAD
        COALESCE(fsm.scheme_id, ms2.scheme_id) AS scheme_id,
        COALESCE(ms.scheme_name, ms2.scheme_name) AS scheme_name,
        COALESCE(ms.isin, ms2.isin) AS isin
    FROM investment.onboard_transaction ot
    LEFT JOIN investment.masterfolio mf 
        ON mf.folio_number = ot.folio_number
    LEFT JOIN investment.masterdemataccount md
        ON md.demat_account_number = ot.demat_acc_number
    LEFT JOIN investment.folioschememapping fsm 
        ON fsm.folio_id = mf.folio_id
=======
        COALESCE(ot.scheme_id, fsm.scheme_id, ms2.scheme_id) AS scheme_id,
        COALESCE(ms2.scheme_name, ms.scheme_name) AS scheme_name,
        COALESCE(ms2.isin, ms.isin) AS isin
    FROM investment.onboard_transaction ot
    LEFT JOIN investment.masterfolio mf 
        ON mf.folio_id = ot.folio_id
    LEFT JOIN investment.masterdemataccount md
        ON md.demat_id = ot.demat_id
    LEFT JOIN investment.folioschememapping fsm 
        ON fsm.folio_id = ot.folio_id
>>>>>>> origin/main
    LEFT JOIN investment.masterscheme ms 
        ON ms.scheme_id = fsm.scheme_id
    LEFT JOIN investment.masterscheme ms2
        ON ms2.internal_scheme_code = ot.scheme_internal_code
    WHERE ot.batch_id = $1
),
transaction_summary AS (
    SELECT
        entity_name,
        folio_number,
        demat_acc_number,
<<<<<<< HEAD
=======
        ot.folio_id,
        ot.demat_id,
>>>>>>> origin/main
        scheme_id,
        scheme_name,
        isin,
        SUM(CASE 
            WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
            WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -units
            ELSE units
        END) AS total_units,
        SUM(CASE 
            WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN amount
            WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -amount
            ELSE amount
        END) AS total_invested_amount,
        CASE 
            WHEN SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
                WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -units
                ELSE units
            END) = 0 THEN 0
            ELSE SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN nav * units
                WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -nav * units
                ELSE nav * units
            END) / SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
                WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -units
                ELSE units
            END)
        END AS avg_nav
    FROM scheme_resolved
<<<<<<< HEAD
    GROUP BY entity_name, folio_number, demat_acc_number, scheme_id, scheme_name, isin
=======
    GROUP BY entity_name, folio_number, demat_acc_number, folio_id, demat_id, scheme_id, scheme_name, isin
>>>>>>> origin/main
),
latest_nav AS (
    SELECT DISTINCT ON (scheme_name)
        scheme_name,
        isin_div_payout_growth as isin,
        nav_value,
        nav_date
    FROM investment.amfi_nav_staging
    ORDER BY scheme_name, nav_date DESC
)
INSERT INTO investment.portfolio_snapshot (
  batch_id,
  entity_name,
  folio_number,
  demat_acc_number,
<<<<<<< HEAD
=======
  folio_id,
  demat_id,
>>>>>>> origin/main
  scheme_id,
  scheme_name,
  isin,
  total_units,
  avg_nav,
  current_nav,
  current_value,
  total_invested_amount,
  gain_loss,
  gain_losss_percent,
  created_at
)
SELECT
  $1::uuid AS batch_id,
  ts.entity_name,
  ts.folio_number,
  ts.demat_acc_number,
<<<<<<< HEAD
=======
  ts.folio_id,
  ts.demat_id,
>>>>>>> origin/main
  ts.scheme_id,
  ts.scheme_name,
  ts.isin,
  ts.total_units,
  ts.avg_nav,
  COALESCE(ln.nav_value, 0) AS current_nav,
  ts.total_units * COALESCE(ln.nav_value, 0) AS current_value,
  ts.total_invested_amount,
  (ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount AS gain_loss,
  CASE 
    WHEN ts.total_invested_amount = 0 THEN 0
    ELSE (((ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount) / ts.total_invested_amount) * 100
  END AS gain_losss_percent,
  NOW()
FROM transaction_summary ts
LEFT JOIN latest_nav ln ON (ln.scheme_name = ts.scheme_name OR ln.isin = ts.isin)
WHERE ts.total_units > 0;
`

		if _, err := pgxPool.Exec(ctx, recalcQ, batchID); err != nil {
			log.Printf("[refresh] recalculate snapshot failed: %v", err)
			api.RespondWithError(w, 500, "recalculate snapshot failed: "+err.Error())
			return
		}

		// Count new snapshots
		var snapshotCnt int64
		if err := pgxPool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.portfolio_snapshot WHERE batch_id=$1`, batchID).Scan(&snapshotCnt); err != nil {
			log.Printf("[refresh] count snapshots failed: %v", err)
			api.RespondWithError(w, 500, "count snapshots failed: "+err.Error())
			return
		}

		log.Printf("[refresh] refreshed %d snapshot rows for batch %s", snapshotCnt, batchID)
		api.RespondWithPayload(w, true, "portfolio snapshot refreshed", map[string]interface{}{
			"batch_id":       batchID,
			"snapshot_count": snapshotCnt,
		})
	}
}

func PostPortfolioSnapshot(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var req struct {
			BatchID string `json:"batch_id"`
		}

		// Support both JSON and form
<<<<<<< HEAD
		contentType := r.Header.Get(constants.ContentTypeText)
		if strings.Contains(contentType, constants.ContentTypeJSON) {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				api.RespondWithError(w, 400, constants.ErrInvalidJSONPrefix+err.Error())
=======
		contentType := r.Header.Get("Content-Type")
		if strings.Contains(contentType, "application/json") {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				api.RespondWithError(w, 400, "invalid JSON: "+err.Error())
>>>>>>> origin/main
				return
			}
		} else {
			_ = r.ParseForm()
			req.BatchID = r.FormValue("batch_id")
		}

		if req.BatchID == "" {
			api.RespondWithError(w, 400, "batch_id required")
			return
		}

		batchID := req.BatchID

		summaryQ := `
        WITH latest_nav AS (
            SELECT DISTINCT ON (scheme_name)
                scheme_name,
                nav_value,
                raw_category_header
            FROM investment.amfi_nav_staging
            ORDER BY scheme_name, nav_date DESC
        ),
        tx_agg AS (
            SELECT 
                ot.folio_number,
                COALESCE(fsm.scheme_id, ms.scheme_id) AS scheme_id,
                ms.scheme_name,
                SUM(ot.amount) AS total_investment,
                SUM(ot.units) AS total_units
            FROM investment.onboard_transaction ot
            LEFT JOIN investment.masterfolio mf ON mf.folio_number = ot.folio_number
            LEFT JOIN investment.folioschememapping fsm ON fsm.folio_id = mf.folio_id
            LEFT JOIN investment.masterscheme ms ON ms.scheme_id = fsm.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code
            WHERE ot.batch_id = $1
            GROUP BY ot.folio_number, COALESCE(fsm.scheme_id, ms.scheme_id), ms.scheme_name
        )
        SELECT 
            COALESCE(SUM(total_investment),0) AS total_investment,
            COALESCE(SUM(total_units * COALESCE(ln.nav_value,0)),0) AS current_value
        FROM tx_agg ta
        LEFT JOIN latest_nav ln ON ln.scheme_name = ta.scheme_name;
        `

		var totalInv, currVal float64
		if err := pgxPool.QueryRow(ctx, summaryQ, batchID).Scan(&totalInv, &currVal); err != nil {
			log.Printf("[dash] summary query failed: %v", err)
			api.RespondWithError(w, 500, "summary query failed: "+err.Error())
			return
		}

		gain := currVal - totalInv
		gainPct := 0.0
		if totalInv != 0 {
			gainPct = (gain / totalInv) * 100
		}

		allocQ := `
        WITH latest_nav AS (
            SELECT DISTINCT ON (scheme_name)
                scheme_name,
                nav_value,
                raw_category_header
            FROM investment.amfi_nav_staging
            ORDER BY scheme_name, nav_date DESC
        ),
        tx_agg AS (
            SELECT 
                ot.folio_number,
                COALESCE(fsm.scheme_id, ms.scheme_id) AS scheme_id,
                ms.scheme_name,
                SUM(ot.units) AS total_units
            FROM investment.onboard_transaction ot
            LEFT JOIN investment.masterfolio mf ON mf.folio_number = ot.folio_number
            LEFT JOIN investment.folioschememapping fsm ON fsm.folio_id = mf.folio_id
            LEFT JOIN investment.masterscheme ms ON ms.scheme_id = fsm.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code
            WHERE ot.batch_id = $1
            GROUP BY ot.folio_number, COALESCE(fsm.scheme_id, ms.scheme_id), ms.scheme_name
        )
        SELECT 
            COALESCE(ln.raw_category_header, 'Uncategorized') AS category,
            SUM(ta.total_units * COALESCE(ln.nav_value,0)) AS value
        FROM tx_agg ta
        LEFT JOIN latest_nav ln ON ln.scheme_name = ta.scheme_name
        GROUP BY ln.raw_category_header;
        `

		allocRows, err := pgxPool.Query(ctx, allocQ, batchID)
		if err != nil {
			log.Printf("[dash] allocation query failed: %v", err)
			api.RespondWithError(w, 500, "allocation query failed: "+err.Error())
			return
		}
		allocation := map[string]float64{}
		for allocRows.Next() {
			var cat sql.NullString
			var val sql.NullFloat64
			_ = allocRows.Scan(&cat, &val)
			key := "Uncategorized"
			if cat.Valid {
				key = cat.String
			}
			allocation[key] = val.Float64
		}
		allocRows.Close()

		holdingsQ := `
        WITH latest_nav AS (
            SELECT DISTINCT ON (scheme_name)
                scheme_name,
                nav_value
            FROM investment.amfi_nav_staging
            ORDER BY scheme_name, nav_date DESC
        ),
        tx_agg AS (
            SELECT 
                ot.folio_number,
                COALESCE(fsm.scheme_id, ms.scheme_id) AS scheme_id,
                ms.scheme_name,
                SUM(ot.units) AS total_units
            FROM investment.onboard_transaction ot
            LEFT JOIN investment.masterfolio mf ON mf.folio_number = ot.folio_number
            LEFT JOIN investment.folioschememapping fsm ON fsm.folio_id = mf.folio_id
            LEFT JOIN investment.masterscheme ms ON ms.scheme_id = fsm.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code
            WHERE ot.batch_id = $1
            GROUP BY ot.folio_number, COALESCE(fsm.scheme_id, ms.scheme_id), ms.scheme_name
        )
        SELECT 
            ta.scheme_name,
            ta.folio_number,
            ta.total_units,
            COALESCE(ln.nav_value,0) AS current_nav,
            ta.total_units * COALESCE(ln.nav_value,0) AS current_value
        FROM tx_agg ta
        LEFT JOIN latest_nav ln ON ln.scheme_name = ta.scheme_name
        ORDER BY current_value DESC;
        `

		rows, err := pgxPool.Query(ctx, holdingsQ, batchID)
		if err != nil {
			log.Printf("[dash] holdings query failed: %v", err)
			api.RespondWithError(w, 500, "holdings query failed: "+err.Error())
			return
		}
		holdings := []Holding{}
		for rows.Next() {
			var h Holding
			if err := rows.Scan(&h.SchemeName, &h.Folio, &h.TotalUnits, &h.CurrentNav, &h.CurrentValue); err == nil {
				holdings = append(holdings, h)
			}
		}
		rows.Close()

		resp := DashboardResponse{
			TotalInvestment: totalInv,
			CurrentValue:    currVal,
			GainLoss:        gain,
			GainLossPct:     gainPct,
			Allocation:      allocation,
			Holdings:        holdings,
		}

		api.RespondWithPayload(w, true, "", resp)
	}
}
