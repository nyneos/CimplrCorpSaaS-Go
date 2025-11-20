package investment

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	Amount             float64
	Units              float64
	Nav                float64
}

// Response structure
type UploadResponse struct {
	Success bool             `json:"success"`
	BatchID string           `json:"batch_id,omitempty"`
	Counts  map[string]int64 `json:"counts,omitempty"`
	Message string           `json:"message,omitempty"`
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
		// parse date flexible formats
		var dt time.Time
		dtStr := strings.TrimSpace(rec[0])
		dt, err = time.Parse("2006-01-02", dtStr)
		if err != nil {
			dt, err = time.Parse("02/01/2006", dtStr)
			if err != nil {
				return nil, fmt.Errorf("invalid date %s", dtStr)
			}
		}
		amount, _ := strconv.ParseFloat(strings.TrimSpace(rec[4]), 64)
		units, _ := strconv.ParseFloat(strings.TrimSpace(rec[5]), 64)
		nav, _ := strconv.ParseFloat(strings.TrimSpace(rec[6]), 64)
		rows = append(rows, TxCSVRow{TransactionDate: dt, TransactionType: strings.TrimSpace(rec[1]), SchemeInternalCode: strings.TrimSpace(rec[2]), FolioNumber: strings.TrimSpace(rec[3]), Amount: amount, Units: units, Nav: nav})
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

		userID := r.FormValue("user_id")
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id required")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}
		log.Printf("[bulk] user %s (%s)", userID, userEmail)

		// begin tx (SERIALIZABLE)
		tx, err := pgxPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			log.Printf("[bulk] tx begin: %v", err)
			api.RespondWithError(w, 500, "tx begin: "+err.Error())
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

		counts := map[string]int64{"amc": 0, "scheme": 0, "dp": 0, "demat": 0, "folio": 0, "transactions": 0, "snapshot": 0}

		// Process AMCs
		log.Printf("[bulk] starting AMC processing, found %d AMCs", len(amcs))
		for i, a := range amcs {
			log.Printf("[bulk] processing amc[%d]: %+v", i, a)
			if a.Enriched && a.AmcID != "" {
				log.Printf("[bulk] amc[%d] is enriched, skipping insert", i)
				amcMap[defaultIfEmpty(a.InternalAmcCode, a.AmcName)] = a.AmcID
				continue
			}
			// lookup by internal code or name
			var existing string
			if a.InternalAmcCode != "" {
				log.Printf("[bulk] amc[%d] checking existing by internal_code: %s", i, a.InternalAmcCode)
				_ = tx.QueryRow(ctx, `SELECT amc_id FROM investment.masteramc WHERE internal_amc_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, a.InternalAmcCode).Scan(&existing)
			}
			if existing == "" {
				log.Printf("[bulk] amc[%d] checking existing by name: %s", i, a.AmcName)
				_ = tx.QueryRow(ctx, `SELECT amc_id FROM investment.masteramc WHERE amc_name=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, a.AmcName).Scan(&existing)
			}
			if existing != "" {
				log.Printf("[bulk] amc[%d] found existing: %s", i, existing)
				amcMap[defaultIfEmpty(a.InternalAmcCode, a.AmcName)] = existing
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
			if d.Enriched && d.DPID != "" {
				dpMap[d.DPCode] = d.DPID
				continue
			}
			var existing string
			if d.DPCode != "" {
				_ = tx.QueryRow(ctx, `SELECT dp_id FROM investment.masterdepositoryparticipant WHERE dp_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, d.DPCode).Scan(&existing)
			}
			if existing != "" {
				dpMap[d.DPCode] = existing
				continue
			}
			var dpID string
			if err := tx.QueryRow(ctx, `INSERT INTO investment.masterdepositoryparticipant (dp_name, dp_code, depository, status, source, batch_id) VALUES ($1,$2,$3,$4,'Manual',$5) RETURNING dp_id`, d.DPName, d.DPCode, defaultIfEmpty(d.Depository, "NSDL"), defaultIfEmpty(d.Status, "Active"), batchID).Scan(&dpID); err != nil {
				log.Printf("[bulk] insert dp failed: %v", err)
				api.RespondWithError(w, 500, "insert dp failed: "+err.Error())
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
			if dm.Enriched && dm.DematID != "" {
				dematMap[dm.DematAccountNumber] = dm.DematID
				continue
			}
			var existing string
			if dm.DematAccountNumber != "" {
				_ = tx.QueryRow(ctx, `SELECT demat_id FROM investment.masterdemataccount WHERE demat_account_number=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, dm.DematAccountNumber).Scan(&existing)
			}
			if existing != "" {
				dematMap[dm.DematAccountNumber] = existing
				continue
			}
			var dematID string
			if err := tx.QueryRow(ctx, `INSERT INTO investment.masterdemataccount (entity_name, dp_id, depository, demat_account_number, depository_participant, client_id, default_settlement_account, status, source, batch_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'Manual',$9) RETURNING demat_id`, dm.EntityName, dm.DPID, defaultIfEmpty(dm.Depository, "NSDL"), dm.DematAccountNumber, dm.DepositoryParticipant, dm.ClientID, defaultIfEmpty(dm.DefaultSettlementAccount, "SYSTEM"), defaultIfEmpty(dm.Status, "Active"), batchID).Scan(&dematID); err != nil {
				log.Printf("[bulk] insert demat failed: %v", err)
				api.RespondWithError(w, 500, "insert demat failed: "+err.Error())
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
				folioMap[f.FolioNumber] = f.FolioID
				continue
			}
			var existing string
			if f.FolioNumber != "" {
				_ = tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number=$1 AND entity_name=$2 AND amc_name=$3 AND COALESCE(is_deleted,false)=false LIMIT 1`, f.FolioNumber, f.EntityName, f.AmcName).Scan(&existing)
			}
			if existing != "" {
				folioMap[f.FolioNumber] = existing
				continue
			}
			var folioID string
			if err := tx.QueryRow(ctx, `INSERT INTO investment.masterfolio (entity_name, amc_name, folio_number, first_holder_name, default_subscription_account, default_redemption_account, status, source, batch_id) VALUES ($1,$2,$3,$4,$5,$6,$7,'Manual',$8) RETURNING folio_id`, f.EntityName, f.AmcName, f.FolioNumber, f.FirstHolderName, defaultIfEmpty(f.DefaultSubscriptionAccount, "SYSTEM"), defaultIfEmpty(f.DefaultRedemptionAccount, "SYSTEM"), defaultIfEmpty(f.Status, "Active"), batchID).Scan(&folioID); err != nil {
				log.Printf("[bulk] insert folio failed: %v", err)
				api.RespondWithError(w, 500, "insert folio failed: "+err.Error())
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
			if s.Enriched && s.SchemeID != "" {
				schemeMap[s.InternalSchemeCode] = s.SchemeID
				continue
			}
			var existing string
			if s.InternalSchemeCode != "" {
				_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE internal_scheme_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.InternalSchemeCode).Scan(&existing)
			}
			if existing == "" {
				_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE isin=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.ISIN).Scan(&existing)
			}
			if existing != "" {
				schemeMap[s.InternalSchemeCode] = existing
				continue
			}
			var schemeID string
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
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'AMC',$3,$4)`, batchID, v, k, true); err != nil {
				log.Printf("[bulk] onboard mapping amc failed: %v", err)
				api.RespondWithError(w, 500, "onboard mapping failed: "+err.Error())
				return
			}
		}
		for k, v := range schemeMap {
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'SCHEME',$3,$4)`, batchID, v, k, true); err != nil {
				log.Printf("[bulk] onboard mapping scheme failed: %v", err)
				api.RespondWithError(w, 500, "onboard mapping failed: "+err.Error())
				return
			}
		}
		for k, v := range dpMap {
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'DP',$3,$4)`, batchID, v, k, true); err != nil {
				log.Printf("[bulk] onboard mapping dp failed: %v", err)
				api.RespondWithError(w, 500, "onboard mapping failed: "+err.Error())
				return
			}
		}
		for k, v := range dematMap {
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'DEMAT',$3,$4)`, batchID, v, k, true); err != nil {
				log.Printf("[bulk] onboard mapping demat failed: %v", err)
				api.RespondWithError(w, 500, "onboard mapping failed: "+err.Error())
				return
			}
		}
		for k, v := range folioMap {
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'FOLIO',$3,$4)`, batchID, v, k, true); err != nil {
				log.Printf("[bulk] onboard mapping folio failed: %v", err)
				api.RespondWithError(w, 500, "onboard mapping failed: "+err.Error())
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

		// Parse transactions CSV if present
		var txRows []TxCSVRow
		if fhArr := r.MultipartForm.File["transactions"]; len(fhArr) > 0 {
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
			// insert transactions -- use prepared multi-row insert in this tx
			for _, tr := range txRows {
				// resolve scheme_id
				var schemeID string
				if v, ok := schemeMap[tr.SchemeInternalCode]; ok {
					schemeID = v
				} else {
					_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE internal_scheme_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, tr.SchemeInternalCode).Scan(&schemeID)
				}
				// resolve folio_id
				var folioID string
				if v, ok := folioMap[tr.FolioNumber]; ok {
					folioID = v
				} else {
					_ = tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, tr.FolioNumber).Scan(&folioID)
				}
				if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_transaction (batch_id, transaction_date, transaction_type, scheme_internal_code, folio_number, amount, units, nav, scheme_id, folio_id, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())`, batchID, tr.TransactionDate, tr.TransactionType, tr.SchemeInternalCode, tr.FolioNumber, tr.Amount, tr.Units, tr.Nav, nullableString(schemeID), nullableString(folioID)); err != nil {
					log.Printf("[bulk] insert onboard_transaction failed: %v", err)
					api.RespondWithError(w, 500, "insert transaction failed: "+err.Error())
					return
				}
				counts["transactions"]++
			}
			log.Printf("[bulk] inserted %d transactions", counts["transactions"])
		}

	
		aggQ := `
WITH scheme_resolved AS (
    SELECT
        ot.batch_id,
        ot.transaction_date,
        ot.transaction_type,
        ot.amount,
        ot.units,
        ot.nav,
        mf.entity_name,
        ot.folio_number,
        COALESCE(fsm.scheme_id, ms2.scheme_id) AS scheme_id,
        COALESCE(ms.scheme_name, ms2.scheme_name) AS scheme_name,
        COALESCE(ms.isin, ms2.isin) AS isin
    FROM investment.onboard_transaction ot
    JOIN investment.masterfolio mf 
        ON mf.folio_number = ot.folio_number
    LEFT JOIN investment.folioschememapping fsm 
        ON fsm.folio_id = mf.folio_id
    LEFT JOIN investment.masterscheme ms 
        ON ms.scheme_id = fsm.scheme_id
    LEFT JOIN investment.masterscheme ms2
        ON ms2.internal_scheme_code = ot.scheme_internal_code
    WHERE ot.batch_id = $1
)
INSERT INTO investment.portfolio_snapshot (
  batch_id,
  entity_name,
  folio_number,
  scheme_id,
  scheme_name,
  isin,
  total_units,
  avg_nav,
  current_nav,
  current_value,
  gain_loss,
  created_at
)
SELECT
  $1 AS batch_id,
  sr.entity_name,
  sr.folio_number,
  sr.scheme_id,
  sr.scheme_name,
  sr.isin,
  SUM(sr.units) AS total_units,
  CASE WHEN SUM(sr.units)=0 THEN 0 ELSE SUM(sr.nav * sr.units)/SUM(sr.units) END AS avg_nav,
  COALESCE(
     (SELECT nav_value 
      FROM investment.amfi_nav_staging an 
      WHERE an.scheme_name = sr.scheme_name 
      ORDER BY an.nav_date DESC 
      LIMIT 1), 0
  ) AS current_nav,
  SUM(sr.units) *
  COALESCE(
     (SELECT nav_value 
      FROM investment.amfi_nav_staging an 
      WHERE an.scheme_name = sr.scheme_name 
      ORDER BY an.nav_date DESC LIMIT 1), 0
  ) AS current_value,
  0 AS gain_loss,
  NOW()
FROM scheme_resolved sr
GROUP BY sr.entity_name, sr.folio_number, sr.scheme_id, sr.scheme_name, sr.isin;
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

		// finalize batch update totals & status
		if _, err := tx.Exec(ctx, `UPDATE investment.onboard_batch SET total_records = (SELECT COUNT(*) FROM investment.onboard_transaction WHERE batch_id=$1), status='COMPLETED', completed_at=now() WHERE batch_id=$1`, batchID); err != nil {
			log.Printf("[bulk] finalize batch failed: %v", err)
			api.RespondWithError(w, 500, "finalize batch failed: "+err.Error())
			return
		}

		// commit
		if err := tx.Commit(ctx); err != nil {
			log.Printf("[bulk] commit failed: %v", err)
			api.RespondWithError(w, 500, "commit failed: "+err.Error())
			return
		}
		tx = nil
		log.Printf("[bulk] committed batch %s", batchID)
		log.Printf("[bulk] final counts: %+v", counts)

		res := UploadResponse{Success: true, BatchID: batchID, Counts: counts, Message: "bulk onboard completed"}
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

func batchInsertAMCs(ctx context.Context, tx pgx.Tx, batchID string, rows []AMCInput, userEmail string) (map[string]string, error) {
	res := map[string]string{}
	if len(rows) == 0 {
		return res, nil
	}
	// create temp table
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE tmp_amc_upload (amc_name text, internal_amc_code text, status text, primary_contact_name text, primary_contact_email text, sebi_registration_no text, amc_beneficiary_name text, amc_bank_account_no text, amc_bank_name text, amc_bank_ifsc text, mfu_amc_code text, cams_amc_code text, erp_vendor_code text) ON COMMIT DROP`); err != nil {
		return nil, err
	}
	// prepare rows for copy
	copyRows := make([][]interface{}, 0, len(rows))
	for _, r := range rows {
		if r.Enriched && r.AmcID != "" {
			res[defaultIfEmpty(r.InternalAmcCode, r.AmcName)] = r.AmcID
			continue
		}
		copyRows = append(copyRows, []interface{}{r.AmcName, r.InternalAmcCode, defaultIfEmpty(r.Status, "Active"), r.PrimaryContactName, r.PrimaryContactEmail, r.SebiRegistrationNo, r.AmcBeneficiaryName, r.AmcBankAccountNo, r.AmcBankName, r.AmcBankIfsc, r.MfuAmcCode, r.CamsAmcCode, r.ErpVendorCode})
	}
	if len(copyRows) == 0 {
		return res, nil
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_amc_upload"}, []string{"amc_name", "internal_amc_code", "status", "primary_contact_name", "primary_contact_email", "sebi_registration_no", "amc_beneficiary_name", "amc_bank_account_no", "amc_bank_name", "amc_bank_ifsc", "mfu_amc_code", "cams_amc_code", "erp_vendor_code"}, pgx.CopyFromRows(copyRows)); err != nil {
		return nil, err
	}
	// Insert missing into masteramc
	insertQ := `INSERT INTO investment.masteramc (amc_name, internal_amc_code, status, primary_contact_name, primary_contact_email, sebi_registration_no, amc_beneficiary_name, amc_bank_account_no, amc_bank_name, amc_bank_ifsc, mfu_amc_code, cams_amc_code, erp_vendor_code, source, batch_id) SELECT tam.amc_name, tam.internal_amc_code, tam.status, tam.primary_contact_name, tam.primary_contact_email, tam.sebi_registration_no, tam.amc_beneficiary_name, tam.amc_bank_account_no, tam.amc_bank_name, tam.amc_bank_ifsc, tam.mfu_amc_code, tam.cams_amc_code, tam.erp_vendor_code, 'Manual', $1 FROM tmp_amc_upload tam WHERE NOT EXISTS (SELECT 1 FROM investment.masteramc m WHERE m.internal_amc_code = tam.internal_amc_code AND COALESCE(m.is_deleted,false)=false)`
	if _, err := tx.Exec(ctx, insertQ, batchID); err != nil {
		return nil, err
	}
	// fetch ids for all internal codes present
	rowsQ := `SELECT amc_id, internal_amc_code, amc_name FROM investment.masteramc WHERE internal_amc_code IN (SELECT internal_amc_code FROM tmp_amc_upload) OR amc_name IN (SELECT amc_name FROM tmp_amc_upload)`
	rset, err := tx.Query(ctx, rowsQ)
	if err != nil {
		return nil, err
	}
	defer rset.Close()
	for rset.Next() {
		var id, code, name string
		_ = rset.Scan(&id, &code, &name)
		key := defaultIfEmpty(code, name)
		res[key] = id
		// insert mapping
		if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'AMC',$3,false)`, batchID, id, key); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func batchInsertDPs(ctx context.Context, tx pgx.Tx, batchID string, rows []DPInput, userEmail string) (map[string]string, error) {
	res := map[string]string{}
	if len(rows) == 0 {
		return res, nil
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE tmp_dp_upload (dp_name text, dp_code text, depository text, status text) ON COMMIT DROP`); err != nil {
		return nil, err
	}
	copyRows := [][]interface{}{}
	for _, r := range rows {
		if r.Enriched && r.DPID != "" {
			res[r.DPCode] = r.DPID
			continue
		}
		copyRows = append(copyRows, []interface{}{r.DPName, r.DPCode, defaultIfEmpty(r.Depository, "NSDL"), defaultIfEmpty(r.Status, "Active")})
	}
	if len(copyRows) == 0 {
		return res, nil
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_dp_upload"}, []string{"dp_name", "dp_code", "depository", "status"}, pgx.CopyFromRows(copyRows)); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO investment.masterdepositoryparticipant (dp_name, dp_code, depository, status, source, batch_id) SELECT t.dp_name, t.dp_code, t.depository, t.status, 'Manual', $1 FROM tmp_dp_upload t WHERE NOT EXISTS (SELECT 1 FROM investment.masterdepositoryparticipant m WHERE m.dp_code = t.dp_code AND COALESCE(m.is_deleted,false)=false)`, batchID); err != nil {
		return nil, err
	}
	rset, err := tx.Query(ctx, `SELECT dp_id, dp_code FROM investment.masterdepositoryparticipant WHERE dp_code IN (SELECT dp_code FROM tmp_dp_upload)`)
	if err != nil {
		return nil, err
	}
	defer rset.Close()
	for rset.Next() {
		var id, code string
		_ = rset.Scan(&id, &code)
		res[code] = id
		if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'DP',$3,false)`, batchID, id, code); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func batchInsertDemats(ctx context.Context, tx pgx.Tx, batchID string, rows []DematInput, userEmail string) (map[string]string, error) {
	res := map[string]string{}
	if len(rows) == 0 {
		return res, nil
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE tmp_demat_upload (entity_name text, dp_id text, depository text, demat_account_number text, depository_participant text, client_id text, default_settlement_account text, status text) ON COMMIT DROP`); err != nil {
		return nil, err
	}
	copyRows := [][]interface{}{}
	for _, r := range rows {
		if r.Enriched && r.DematID != "" {
			res[r.DematAccountNumber] = r.DematID
			continue
		}
		copyRows = append(copyRows, []interface{}{r.EntityName, r.DPID, defaultIfEmpty(r.Depository, "NSDL"), r.DematAccountNumber, r.DepositoryParticipant, r.ClientID, defaultIfEmpty(r.DefaultSettlementAccount, "SYSTEM"), defaultIfEmpty(r.Status, "Active")})
	}
	if len(copyRows) == 0 {
		return res, nil
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_demat_upload"}, []string{"entity_name", "dp_id", "depository", "demat_account_number", "depository_participant", "client_id", "default_settlement_account", "status"}, pgx.CopyFromRows(copyRows)); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO investment.masterdemataccount (entity_name, dp_id, depository, demat_account_number, depository_participant, client_id, default_settlement_account, status, source, batch_id) SELECT t.entity_name, t.dp_id, t.depository, t.demat_account_number, t.depository_participant, t.client_id, t.default_settlement_account, t.status, 'Manual', $1 FROM tmp_demat_upload t WHERE NOT EXISTS (SELECT 1 FROM investment.masterdemataccount m WHERE m.demat_account_number = t.demat_account_number AND COALESCE(m.is_deleted,false)=false)`, batchID); err != nil {
		return nil, err
	}
	rset, err := tx.Query(ctx, `SELECT demat_id, demat_account_number FROM investment.masterdemataccount WHERE demat_account_number IN (SELECT demat_account_number FROM tmp_demat_upload)`)
	if err != nil {
		return nil, err
	}
	defer rset.Close()
	for rset.Next() {
		var id, acc string
		_ = rset.Scan(&id, &acc)
		res[acc] = id
		if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'DEMAT',$3,false)`, batchID, id, acc); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func batchInsertFolios(ctx context.Context, tx pgx.Tx, batchID string, rows []FolioInput, userEmail string) (map[string]string, error) {
	res := map[string]string{}
	if len(rows) == 0 {
		return res, nil
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE tmp_folio_upload (entity_name text, amc_name text, folio_number text, first_holder_name text, default_subscription_account text, default_redemption_account text, status text) ON COMMIT DROP`); err != nil {
		return nil, err
	}
	copyRows := [][]interface{}{}
	for _, r := range rows {
		if r.Enriched && r.FolioID != "" {
			res[r.FolioNumber] = r.FolioID
			continue
		}
		copyRows = append(copyRows, []interface{}{r.EntityName, r.AmcName, r.FolioNumber, r.FirstHolderName, defaultIfEmpty(r.DefaultSubscriptionAccount, "SYSTEM"), defaultIfEmpty(r.DefaultRedemptionAccount, "SYSTEM"), defaultIfEmpty(r.Status, "Active")})
	}
	if len(copyRows) == 0 {
		return res, nil
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_folio_upload"}, []string{"entity_name", "amc_name", "folio_number", "first_holder_name", "default_subscription_account", "default_redemption_account", "status"}, pgx.CopyFromRows(copyRows)); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO investment.masterfolio (entity_name, amc_name, folio_number, first_holder_name, default_subscription_account, default_redemption_account, status, source, batch_id) SELECT t.entity_name, t.amc_name, t.folio_number, t.first_holder_name, t.default_subscription_account, t.default_redemption_account, t.status, 'Manual', $1 FROM tmp_folio_upload t WHERE NOT EXISTS (SELECT 1 FROM investment.masterfolio m WHERE m.entity_name = t.entity_name AND m.amc_name = t.amc_name AND m.folio_number = t.folio_number AND COALESCE(m.is_deleted,false)=false)`, batchID); err != nil {
		return nil, err
	}
	// fetch folio ids
	rset, err := tx.Query(ctx, `SELECT folio_id, folio_number FROM investment.masterfolio WHERE folio_number IN (SELECT folio_number FROM tmp_folio_upload)`)
	if err != nil {
		return nil, err
	}
	defer rset.Close()
	for rset.Next() {
		var id, fnum string
		_ = rset.Scan(&id, &fnum)
		res[fnum] = id
		if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'FOLIO',$3,false)`, batchID, id, fnum); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func batchInsertSchemes(ctx context.Context, tx pgx.Tx, batchID string, rows []SchemeInput, userEmail string) (map[string]string, error) {
	res := map[string]string{}
	if len(rows) == 0 {
		return res, nil
	}
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE tmp_scheme_upload (scheme_name text, isin text, amc_name text, internal_scheme_code text, internal_risk_rating text, erp_gl_account text, status text) ON COMMIT DROP`); err != nil {
		return nil, err
	}
	copyRows := [][]interface{}{}
	for _, r := range rows {
		if r.Enriched && r.SchemeID != "" {
			res[r.InternalSchemeCode] = r.SchemeID
			continue
		}
		copyRows = append(copyRows, []interface{}{r.SchemeName, r.ISIN, r.AmcName, r.InternalSchemeCode, defaultIfEmpty(r.InternalRiskRating, "Unknown"), r.ErpGlAccount, defaultIfEmpty(r.Status, "Active")})
	}
	if len(copyRows) == 0 {
		return res, nil
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_scheme_upload"}, []string{"scheme_name", "isin", "amc_name", "internal_scheme_code", "internal_risk_rating", "erp_gl_account", "status"}, pgx.CopyFromRows(copyRows)); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO investment.masterscheme (scheme_name, isin, amc_name, internal_scheme_code, internal_risk_rating, erp_gl_account, status, source, batch_id) SELECT t.scheme_name, t.isin, t.amc_name, t.internal_scheme_code, t.internal_risk_rating, t.erp_gl_account, t.status, 'Manual', $1 FROM tmp_scheme_upload t WHERE NOT EXISTS (SELECT 1 FROM investment.masterscheme m WHERE m.internal_scheme_code = t.internal_scheme_code AND COALESCE(m.is_deleted,false)=false)`, batchID); err != nil {
		return nil, err
	}
	rset, err := tx.Query(ctx, `SELECT scheme_id, internal_scheme_code FROM investment.masterscheme WHERE internal_scheme_code IN (SELECT internal_scheme_code FROM tmp_scheme_upload) OR isin IN (SELECT isin FROM tmp_scheme_upload)`)
	if err != nil {
		return nil, err
	}
	defer rset.Close()
	for rset.Next() {
		var id, code string
		_ = rset.Scan(&id, &code)
		res[code] = id
		if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'SCHEME',$3,false)`, batchID, id, code); err != nil {
			return nil, err
		}
	}
	return res, nil
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


func PostPortfolioSnapshot(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var req struct {
			BatchID string `json:"batch_id"`
		}

		// Support both JSON and form
		contentType := r.Header.Get("Content-Type")
		if strings.Contains(contentType, "application/json") {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				api.RespondWithError(w, 400, "invalid JSON: "+err.Error())
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
