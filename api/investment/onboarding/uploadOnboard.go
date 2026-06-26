package investment

import (
	"CimplrCorpSaas/api"

	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/portfolio"
	"CimplrCorpSaas/api/investment/schemejoin"
	_ "CimplrCorpSaas/api/notification/catalog"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/validation"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
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
	Method             string `json:"method,omitempty"`
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
	Success        bool             `json:"success"`
	BatchID        string           `json:"batch_id,omitempty"`
	Counts         map[string]int64 `json:"counts,omitempty"`
	EnrichedCounts map[string]int64 `json:"enriched_counts,omitempty"`
	Message        string           `json:"message,omitempty"`
}

// helper
func defaultIfEmpty(val, def string) string {
	if strings.TrimSpace(val) == "" {
		return def
	}
	return val
}

// Validation helpers (mirrors amcMaster package, kept local to avoid cross-package coupling)
func isAlphanumericOnboard(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
			return false
		}
	}
	return true
}

func isValidIFSCOnboard(s string) bool {
	if len(s) != 11 {
		return false
	}
	for i, c := range s {
		switch {
		case i < 4:
			if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
				return false
			}
		case i == 4:
			if c != '0' {
				return false
			}
		default:
			if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
				return false
			}
		}
	}
	return true
}

func isValidEmailOnboard(s string) bool {
	at := strings.Index(s, "@")
	if at < 1 {
		return false
	}
	domain := s[at+1:]
	return strings.Contains(domain, ".") && len(domain) > 2
}

// isBlankRecord returns true when every meaningful string field is empty (UI placeholder rows).
func isBlankAMC(a AMCInput) bool {
	return strings.TrimSpace(a.AmcName) == "" && strings.TrimSpace(a.InternalAmcCode) == ""
}
func isBlankScheme(s SchemeInput) bool {
	return strings.TrimSpace(s.SchemeName) == "" && strings.TrimSpace(s.AmcName) == ""
}
func isBlankFolio(f FolioInput) bool {
	return strings.TrimSpace(f.FolioNumber) == "" && strings.TrimSpace(f.EntityName) == ""
}
func isBlankDP(d DPInput) bool {
	return strings.TrimSpace(d.DPName) == "" && strings.TrimSpace(d.DPCode) == ""
}
func isBlankDemat(dm DematInput) bool {
	return strings.TrimSpace(dm.EntityName) == "" && strings.TrimSpace(dm.DematAccountNumber) == ""
}

// resolveFolioAmcName picks AMC for a folio when the upload row leaves amc_name blank.
// Prefers AMC from new (non-enriched) schemes in the same batch upload.
func resolveFolioAmcName(f FolioInput, schemes []SchemeInput, amcMap map[string]string) string {
	if name := strings.TrimSpace(f.AmcName); name != "" {
		return name
	}
	seen := make(map[string]struct{})
	var candidates []string
	for _, s := range schemes {
		if s.Enriched {
			continue
		}
		if name := strings.TrimSpace(s.AmcName); name != "" {
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				candidates = append(candidates, name)
			}
		}
	}
	if len(candidates) == 1 {
		return candidates[0]
	}
	if len(candidates) > 1 {
		sort.Strings(candidates)
		return candidates[0]
	}
	var amcNames []string
	for k := range amcMap {
		amcNames = append(amcNames, k)
	}
	sort.Strings(amcNames)
	if len(amcNames) > 0 {
		return amcNames[0]
	}
	return "Unknown AMC"
}

// validateAMCRequired mirrors the AMC master create validations for new (non-enriched) AMCs.
func validateAMCRequired(i int, a AMCInput) string {
	if strings.TrimSpace(a.AmcName) == "" {
		return fmt.Sprintf("AMC #%d: amc_name is required", i+1)
	}
	if a.Enriched {
		return "" // enriched AMCs already exist in the system; skip further validation
	}
	code := strings.TrimSpace(a.InternalAmcCode)
	if code == "" {
		return fmt.Sprintf("AMC #%d '%s': internal_amc_code is required", i+1, a.AmcName)
	}
	if !isAlphanumericOnboard(code) {
		return fmt.Sprintf("AMC #%d '%s': internal_amc_code must be alphanumeric, got '%s'", i+1, a.AmcName, code)
	}
	if mfu := strings.TrimSpace(a.MfuAmcCode); mfu != "" && !isAlphanumericOnboard(mfu) {
		return fmt.Sprintf("AMC #%d '%s': mfu_amc_code must be alphanumeric, got '%s'", i+1, a.AmcName, mfu)
	}
	if cams := strings.TrimSpace(a.CamsAmcCode); cams != "" && !isAlphanumericOnboard(cams) {
		return fmt.Sprintf("AMC #%d '%s': cams_amc_code must be alphanumeric, got '%s'", i+1, a.AmcName, cams)
	}
	if ifsc := strings.ToUpper(strings.TrimSpace(a.AmcBankIfsc)); ifsc != "" && ifsc != "DEFAULT0000" && !isValidIFSCOnboard(ifsc) {
		return fmt.Sprintf("AMC #%d '%s': amc_bank_ifsc '%s' is not a valid IFSC code (format: 4 letters + 0 + 6 alphanumeric)", i+1, a.AmcName, ifsc)
	}
	if email := strings.TrimSpace(a.PrimaryContactEmail); email != "" && !isValidEmailOnboard(email) {
		return fmt.Sprintf("AMC #%d '%s': primary_contact_email '%s' is not a valid email address", i+1, a.AmcName, email)
	}
	return ""
}

// validateSchemeRequired mirrors the scheme master: scheme_name + amc_name + internal_scheme_code required for new.
func validateSchemeRequired(i int, s SchemeInput) string {
	if strings.TrimSpace(s.SchemeName) == "" {
		return fmt.Sprintf("Scheme #%d: scheme_name is required", i+1)
	}
	if strings.TrimSpace(s.AmcName) == "" {
		return fmt.Sprintf("Scheme #%d '%s': amc_name is required", i+1, s.SchemeName)
	}
	if !s.Enriched && strings.TrimSpace(s.InternalSchemeCode) == "" {
		return fmt.Sprintf("Scheme #%d '%s': internal_scheme_code is required for new schemes", i+1, s.SchemeName)
	}
	if !s.Enriched && strings.TrimSpace(s.Method) != "" {
		m := strings.ToUpper(strings.TrimSpace(s.Method))
		if m != "FIFO" && m != "LIFO" && m != "WEIGHTED_AVERAGE" {
			return fmt.Sprintf("Scheme #%d '%s': method must be FIFO, LIFO, or WEIGHTED_AVERAGE", i+1, s.SchemeName)
		}
	}
	return ""
}

// validateFolioRequired: entity_name + folio_number required; subscription/redemption accounts required for new folios.
func validateFolioRequired(i int, f FolioInput) string {
	if strings.TrimSpace(f.EntityName) == "" {
		return fmt.Sprintf("Folio #%d: entity_name is required", i+1)
	}
	if strings.TrimSpace(f.FolioNumber) == "" {
		return fmt.Sprintf("Folio #%d (entity: %s): folio_number is required", i+1, f.EntityName)
	}
	if !f.Enriched && strings.TrimSpace(f.FolioID) == "" {
		if strings.TrimSpace(f.DefaultSubscriptionAccount) == "" {
			return fmt.Sprintf("Folio #%d '%s': default_subscription_account is required", i+1, f.FolioNumber)
		}
		if strings.TrimSpace(f.DefaultRedemptionAccount) == "" {
			return fmt.Sprintf("Folio #%d '%s': default_redemption_account is required", i+1, f.FolioNumber)
		}
	}
	return ""
}

// validateDPRequired: dp_name + dp_code + depository required.
func validateDPRequired(i int, d DPInput) string {
	if strings.TrimSpace(d.DPName) == "" {
		return fmt.Sprintf("DP #%d: dp_name is required", i+1)
	}
	if strings.TrimSpace(d.DPCode) == "" {
		return fmt.Sprintf("DP #%d '%s': dp_code is required", i+1, d.DPName)
	}
	if strings.TrimSpace(d.Depository) == "" {
		return fmt.Sprintf("DP #%d '%s': depository is required", i+1, d.DPName)
	}
	return ""
}

// validateDematRequired: entity_name + demat_account_number + settlement account required for new demats.
func validateDematRequired(i int, dm DematInput) string {
	if strings.TrimSpace(dm.EntityName) == "" {
		return fmt.Sprintf("Demat #%d: entity_name is required", i+1)
	}
	if strings.TrimSpace(dm.DematAccountNumber) == "" {
		return fmt.Sprintf("Demat #%d (entity: %s): demat_account_number is required", i+1, dm.EntityName)
	}
	if !dm.Enriched && strings.TrimSpace(dm.DefaultSettlementAccount) == "" {
		return fmt.Sprintf("Demat #%d '%s': default_settlement_account is required", i+1, dm.DematAccountNumber)
	}
	return ""
}

func validateOnboardingUploadScope(ctx context.Context, w http.ResponseWriter, amcs []AMCInput, schemes []SchemeInput, demats []DematInput, folios []FolioInput) bool {
	// AMC, Scheme, DP: identity is NOT validated against the Approved* context here.
	//   - enriched=true  → entity exists in system but may be PENDING_APPROVAL; Approved* check would block it
	//   - enriched=false → being created fresh; not in system yet, Approved* check would always fail
	//   Field/format validation for these is handled by validateAMCRequired / validateSchemeRequired / validateDPRequired.
	//
	// Demat + Folio: always check entity scope (entity_name) so the upload can't reference
	// entities outside the user's authorized scope.
	//   - enriched=true  → skip Demat/Folio identity check (may be PENDING_APPROVAL)
	//   - enriched=false + has ID → also skip; field validation handles it separately
	//   - scheme_ids on folios: never validated here; schemes may be freshly enriched (PENDING_APPROVAL)
	rows := make([]map[string]interface{}, 0, len(demats)+len(folios))
	for _, d := range demats {
		rows = append(rows, map[string]interface{}{
			"entity_name": d.EntityName,
		})
	}
	for _, f := range folios {
		rows = append(rows, map[string]interface{}{
			"entity_name": f.EntityName,
		})
	}
	for _, row := range rows {
		if msg := validation.ValidateMFMasterReferences(ctx, row); msg != "" {
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return false
		}
	}
	return true
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

	// Build a map of normalized column name to index
	colIdx := make(map[string]int)
	for i, h := range hdr {
		normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(h, " ", ""), "_", ""))
		colIdx[normalized] = i
	}

	getVal := func(rec []string, possibleNames ...string) string {
		for _, name := range possibleNames {
			if idx, ok := colIdx[name]; ok && idx < len(rec) {
				return strings.TrimSpace(rec[idx])
			}
		}
		return ""
	}

	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// Check if row is entirely empty
		isEmpty := true
		for _, v := range rec {
			if strings.TrimSpace(v) != "" {
				isEmpty = false
				break
			}
		}
		if isEmpty {
			continue
		}

		dtStr := getVal(rec, "transactiondate", "date")
		if dtStr == "" {
			return nil, fmt.Errorf("transaction date is missing")
		}
		normalizedDate, err := NormalizeDate(dtStr)
		if err != nil {
			return nil, fmt.Errorf("invalid date %s: %v", dtStr, err)
		}
		dt, err := time.Parse(constants.DateFormat, normalizedDate)
		if err != nil {
			return nil, fmt.Errorf("failed to parse normalized date %s: %v", normalizedDate, err)
		}

		folioNumber := getVal(rec, "folionumber", "folio")
		dematAccNumber := getVal(rec, "demataccountnumber", "demataccnumber", "demat")

		if folioNumber == "" && dematAccNumber == "" {
			return nil, fmt.Errorf("either folio_number or demat_acc_number must be provided")
		}

		amount, _ := strconv.ParseFloat(getVal(rec, "amount"), 64)
		units, _ := strconv.ParseFloat(getVal(rec, "units"), 64)
		nav, _ := strconv.ParseFloat(getVal(rec, "nav"), 64)

		rows = append(rows, TxCSVRow{
			TransactionDate:    dt,
			TransactionType:    getVal(rec, "transactiontype", "type"),
			SchemeInternalCode: getVal(rec, "schemeinternalcode", "scheme"),
			FolioNumber:        folioNumber,
			DematAccNumber:     dematAccNumber,
			Amount:             amount,
			Units:              units,
			Nav:                nav,
		})
	}
	return rows, nil
}

// Main handler
func UploadInvestmentBulkk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger.LogInfo("[bulk] start processing upload")

		// parse multipart
		if err := r.ParseMultipartForm(100 << 20); err != nil {
			logger.LogError("[bulk] parse multipart: %v", err)
			api.RespondWithError(w, http.StatusBadRequest, "form parse failed: "+err.Error())
			return
		}

		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		userEmail := api.GetUserNameFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		logger.LogInfo("[bulk] user %s (%s)", userID, userEmail)

		// begin tx (SERIALIZABLE)
		tx, err := pgxPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			logger.LogError("[bulk] tx begin: %v", err)
			api.RespondWithError(w, 500, constants.ErrTxBegin+err.Error())
			return
		}
		defer func() {
			if tx != nil {
				_ = tx.Rollback(ctx)
				logger.LogInfo("[bulk] tx rolled back in defer")
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
					logger.LogError("[bulk] create batch with provided ID failed: %v", err)
					api.RespondWithError(w, 500, "create batch failed: "+err.Error())
					return
				}
			}
		} else {
			// Create new batch with auto-generated ID
			if err := tx.QueryRow(ctx, `INSERT INTO investment.onboard_batch (user_id, user_email, source, total_records, status) VALUES ($1,$2,'Workbench',0,'IN_PROGRESS') RETURNING batch_id`, userID, userEmail).Scan(&batchID); err != nil {
				logger.LogError("[bulk] create batch failed: %v", err)
				api.RespondWithError(w, 500, "create batch failed: "+err.Error())
				return
			}
		}
		log.Printf("[bulk] using batch %s", batchID)
		uploadS3Key, uploadErr := s3storage.UploadFirstMultipartFile(ctx, "investment-onboarding", userEmail, s3storage.CollectMultipartFiles(r.MultipartForm, "files", "transactions"))
		if uploadErr != nil {
			log.Printf("[bulk] upload original file to s3 failed: %v", uploadErr)
			api.RespondWithError(w, http.StatusInternalServerError, "upload file failed: "+uploadErr.Error())
			return
		}
		logger.LogInfo("[bulk] uploaded original file to s3: %s", uploadS3Key)
		logger.LogInfo("[bulk] using batch %s", batchID)
		var amcs []AMCInput
		var schemes []SchemeInput
		var dps []DPInput
		var demats []DematInput
		var folios []FolioInput

		if fhArr := r.MultipartForm.File["payload"]; len(fhArr) > 0 {
			payloadFile, err := fhArr[0].Open()
			if err != nil {
				logger.LogError("[bulk] failed to open payload file: %v", err)
			} else {
				var payload struct {
					AMC    []AMCInput    `json:"amc"`
					Scheme []SchemeInput `json:"scheme"`
					DP     []DPInput     `json:"dp"`
					Demat  []DematInput  `json:"demat"`
					Folio  []FolioInput  `json:"folio"`
				}
				if err := json.NewDecoder(payloadFile).Decode(&payload); err != nil {
					logger.LogError("[bulk] failed to decode payload: %v", err)
				} else {
					amcs = payload.AMC
					schemes = payload.Scheme
					dps = payload.DP
					demats = payload.Demat
					folios = payload.Folio
					logger.LogInfo("[bulk] parsed payload file: amc=%d scheme=%d dp=%d demat=%d folio=%d",
						len(amcs), len(schemes), len(dps), len(demats), len(folios))
				}
				payloadFile.Close()
			}
		} else {
			// fallback: old style form fields
			mf := r.MultipartForm
			logger.LogInfo("[bulk] multipart form values: %+v", mf.Value)

			// Debug: Print raw form values for each field
			if amcRaw, exists := mf.Value["amc"]; exists && len(amcRaw) > 0 {
				logger.LogInfo("[bulk] raw amc data: %s", amcRaw[0])
			}
			if schemeRaw, exists := mf.Value["scheme"]; exists && len(schemeRaw) > 0 {
				logger.LogInfo("[bulk] raw scheme data: %s", schemeRaw[0])
			}

			var err error
			amcs, err = parseJSONField[AMCInput](mf, "amc")
			if err != nil {
				logger.LogError("[bulk] parse amc failed: %v", err)
			}
			schemes, err = parseJSONField[SchemeInput](mf, "scheme")
			if err != nil {
				logger.LogError("[bulk] parse scheme failed: %v", err)
			}
			dps, err = parseJSONField[DPInput](mf, "dp")
			if err != nil {
				logger.LogError("[bulk] parse dp failed: %v", err)
			}
			demats, err = parseJSONField[DematInput](mf, "demat")
			if err != nil {
				logger.LogError("[bulk] parse demat failed: %v", err)
			}
			folios, err = parseJSONField[FolioInput](mf, "folio")
			if err != nil {
				logger.LogError("[bulk] parse folio failed: %v", err)
			}
			logger.LogInfo("[bulk] parsed arrays (legacy form): amc=%d scheme=%d dp=%d demat=%d folio=%d",
				len(amcs), len(schemes), len(dps), len(demats), len(folios))
		}
		if !validateOnboardingUploadScope(ctx, w, amcs, schemes, demats, folios) {
			return
		}

		// maps to hold created or existing ids
		amcMap := map[string]string{}    // key: internal_amc_code or amc_name -> amc_id
		schemeMap := map[string]string{} // key: internal_scheme_code -> scheme_id
		dpMap := map[string]string{}     // key: dp_code -> dp_id
		dematMap := map[string]string{}  // key: demat_account_number -> demat_id
		folioMap := map[string]string{}  // key: folio_number -> folio_id

		// Track which entities are enriched (pre-existing)
		enrichedAMCs := make(map[string]bool)
		enrichedSchemes := make(map[string]bool)
		enrichedDPs := make(map[string]bool)
		enrichedDemats := make(map[string]bool)
		enrichedFolios := make(map[string]bool)

		counts := map[string]int64{"amc": 0, "scheme": 0, "dp": 0, "demat": 0, "folio": 0, "transactions": 0, "snapshot": 0}
		enrichedCounts := map[string]int64{"amc": 0, "scheme": 0, "dp": 0, "demat": 0, "folio": 0}

		// Process AMCs
		logger.LogInfo("[bulk] starting AMC processing, found %d AMCs", len(amcs))
		for i, a := range amcs {
			if isBlankAMC(a) {
				logger.LogInfo("[bulk] amc[%d] skipped: blank placeholder row", i)
				continue
			}
			if msg := validateAMCRequired(i, a); msg != "" {
				api.RespondWithError(w, http.StatusBadRequest, msg)
				return
			}
			logger.LogInfo("[bulk] processing amc[%d]: %+v", i, a)
			// lookup by internal code or name
			var existing string
			if a.InternalAmcCode != "" {
				logger.LogInfo("[bulk] amc[%d] checking existing by internal_code: %s", i, a.InternalAmcCode)
				_ = tx.QueryRow(ctx, `SELECT amc_id FROM investment.masteramc WHERE internal_amc_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, a.InternalAmcCode).Scan(&existing)
			}
			if existing == "" && a.AmcName != "" {
				logger.LogInfo("[bulk] amc[%d] checking existing by name: %s", i, a.AmcName)
				_ = tx.QueryRow(ctx, `SELECT amc_id FROM investment.masteramc WHERE amc_name=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, a.AmcName).Scan(&existing)
			}
			if existing != "" {
				logger.LogInfo("[bulk] amc[%d] found existing: %s", i, existing)
				key := defaultIfEmpty(a.InternalAmcCode, a.AmcName)
				amcMap[key] = existing
				// Create mapping entry instead of updating batch_id
				_, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, amc_id) VALUES ($1, $2)`, batchID, existing)
				if err != nil {
					logger.LogError("[bulk] amc[%d] mapping insert failed: %v", i, err)
					api.RespondWithError(w, 500, "amc mapping failed: "+err.Error())
					return
				}
				logger.LogInfo("[bulk] amc[%d] created mapping for existing entity", i)
				// If enriched, don't create audit record
				if a.Enriched {
					enrichedCounts["amc"]++
					enrichedAMCs[key] = true
					continue
				}
				// Otherwise, treat as duplicate
				continue
			}
			// insert
			var amcID string
			q := `INSERT INTO investment.masteramc (amc_name, internal_amc_code, status, primary_contact_name, primary_contact_email, sebi_registration_no, amc_beneficiary_name, amc_bank_account_no, amc_bank_name, amc_bank_ifsc, mfu_amc_code, cams_amc_code, erp_vendor_code, source) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'Manual') RETURNING amc_id`
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
				defaultIfEmpty(a.ErpVendorCode, "ERP000")).Scan(&amcID); err != nil {
				logger.LogError("[bulk] insert amc failed: %v", err)
				api.RespondWithError(w, 500, "insert amc failed: "+err.Error())
				return
			}
			logger.LogInfo("[bulk] inserted amc[%d] %s (%s) with batch_id %s", i, amcID, a.AmcName, batchID)
			// Create mapping entry
			if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, amc_id) VALUES ($1, $2)`, batchID, amcID); err != nil {
				logger.LogError("[bulk] amc[%d] mapping insert failed: %v", i, err)
				api.RespondWithError(w, 500, "amc mapping failed: "+err.Error())
				return
			}
			// audit
			if _, err := tx.Exec(ctx, `INSERT INTO investment.auditactionamc (amc_id, actiontype, processing_status, requested_by, requested_at, requested_ip) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3)`, amcID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
				logger.LogError("[bulk] audit amc[%d] insert failed: %v", i, err)
				api.RespondWithError(w, 500, "audit amc failed: "+err.Error())
				return
			}
			logger.LogInfo("[bulk] created audit for amc[%d]: %s", i, amcID)
			amcMap[defaultIfEmpty(a.InternalAmcCode, a.AmcName)] = amcID
			counts["amc"]++
			logger.LogInfo("[bulk] amc[%d] completed, count now: %d", i, counts["amc"])
		}
		logger.LogInfo("[bulk] AMC processing completed. Total processed: %d, Total created: %d", len(amcs), counts["amc"])

		// Process DP
		for i, d := range dps {
			if isBlankDP(d) {
				logger.LogInfo("[bulk] dp[%d] skipped: blank placeholder row", i)
				continue
			}
			if msg := validateDPRequired(i, d); msg != "" {
				api.RespondWithError(w, http.StatusBadRequest, msg)
				return
			}
			logger.LogInfo("[bulk] processing dp: %+v", d)
			var existing string
			if d.DPCode != "" {
				_ = tx.QueryRow(ctx, `SELECT dp_id FROM investment.masterdepositoryparticipant WHERE dp_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, d.DPCode).Scan(&existing)
			}
			if existing != "" {
				dpMap[d.DPCode] = existing
				// Create mapping entry
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name) VALUES ($1, $2)`, batchID, d.DPName); err != nil {
					logger.LogError("[bulk] dp mapping insert failed: %v", err)
					api.RespondWithError(w, 500, "dp mapping failed: "+err.Error())
					return
				}
				if d.Enriched {
					enrichedCounts["dp"]++
					enrichedDPs[d.DPCode] = true
				}
				continue
			}
			var dpID string
			if err := tx.QueryRow(ctx, `INSERT INTO investment.masterdepositoryparticipant (dp_name, dp_code, depository, status, source) VALUES ($1,$2,$3,$4,'Manual') RETURNING dp_id`, d.DPName, d.DPCode, defaultIfEmpty(d.Depository, "NSDL"), defaultIfEmpty(d.Status, "Active")).Scan(&dpID); err != nil {
				logger.LogError("[bulk] insert dp failed: %v", err)
				api.RespondWithError(w, 500, "insert dp failed: "+err.Error())
				return
			}
			// Create mapping entry
			if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name) VALUES ($1, $2)`, batchID, d.DPName); err != nil {
				logger.LogError("[bulk] dp mapping insert failed: %v", err)
				api.RespondWithError(w, 500, "dp mapping failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `INSERT INTO investment.auditactiondp (dp_id, actiontype, processing_status, requested_by, requested_at, requested_ip) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3)`, dpID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
				logger.LogError("[bulk] audit dp failed: %v", err)
				api.RespondWithError(w, 500, "audit dp failed: "+err.Error())
				return
			}
			dpMap[d.DPCode] = dpID
			counts["dp"]++
		}

		// Process Demat
		for i, dm := range demats {
			if isBlankDemat(dm) {
				logger.LogInfo("[bulk] demat[%d] skipped: blank placeholder row", i)
				continue
			}
			if msg := validateDematRequired(i, dm); msg != "" {
				api.RespondWithError(w, http.StatusBadRequest, msg)
				return
			}
			logger.LogInfo("[bulk] processing demat: %+v", dm)
			var existing string
			if dm.DematAccountNumber != "" {
				_ = tx.QueryRow(ctx, `SELECT demat_id FROM investment.masterdemataccount WHERE demat_account_number=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, dm.DematAccountNumber).Scan(&existing)
			}
			if existing != "" {
				dematMap[dm.DematAccountNumber] = existing
				// Create mapping entry
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, demat_id) VALUES ($1, $2, $3)`, batchID, dm.EntityName, existing); err != nil {
					logger.LogError("[bulk] demat mapping insert failed: %v", err)
					api.RespondWithError(w, 500, "demat mapping failed: "+err.Error())
					return
				}
				if dm.Enriched {
					enrichedCounts["demat"]++
					enrichedDemats[dm.DematAccountNumber] = true
				}
				continue
			}
			var dematID string
			if err := tx.QueryRow(ctx, `INSERT INTO investment.masterdemataccount (entity_name, dp_id, depository, demat_account_number, depository_participant, client_id, default_settlement_account, status, source) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'Manual') RETURNING demat_id`, dm.EntityName, dm.DPID, defaultIfEmpty(dm.Depository, "NSDL"), dm.DematAccountNumber, dm.DepositoryParticipant, dm.ClientID, defaultIfEmpty(dm.DefaultSettlementAccount, "SYSTEM"), defaultIfEmpty(dm.Status, "Active")).Scan(&dematID); err != nil {
				logger.LogError("[bulk] insert demat failed: %v", err)
				api.RespondWithError(w, 500, "insert demat failed: "+err.Error())
				return
			}
			// Create mapping entry
			if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, demat_id) VALUES ($1, $2, $3)`, batchID, dm.EntityName, dematID); err != nil {
				logger.LogError("[bulk] demat mapping insert failed: %v", err)
				api.RespondWithError(w, 500, "demat mapping failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, requested_by, requested_at, requested_ip) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3)`, dematID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
				logger.LogError("[bulk] audit demat failed: %v", err)
				api.RespondWithError(w, 500, "audit demat failed: "+err.Error())
				return
			}
			dematMap[dm.DematAccountNumber] = dematID
			counts["demat"]++
		}

		// Process Folios
		for i, f := range folios {
			if isBlankFolio(f) {
				logger.LogInfo("[bulk] folio[%d] skipped: blank placeholder row", i)
				continue
			}
			if msg := validateFolioRequired(i, f); msg != "" {
				api.RespondWithError(w, http.StatusBadRequest, msg)
				return
			}
			f.EntityName = strings.TrimSpace(f.EntityName)
			f.FolioNumber = strings.TrimSpace(f.FolioNumber)
			f.AmcName = strings.TrimSpace(f.AmcName)
			logger.LogInfo("[bulk] processing folio: %+v", f)
			if f.Enriched && f.FolioID != "" {
				logger.LogInfo("[bulk] folio is enriched with ID, using: %s", f.FolioID)
				folioMap[f.FolioNumber] = f.FolioID
				// Create mapping entry
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, folio_id, folio_number) VALUES ($1, $2, $3, $4)`, batchID, f.EntityName, f.FolioID, f.FolioNumber); err != nil {
					logger.LogError(constants.ErrBulkFolioMappingInsertFailed, err)
					api.RespondWithError(w, 500, constants.ErrBulkFolioMappingFailed+err.Error())
					return
				}
				enrichedCounts["folio"]++
				enrichedFolios[f.FolioNumber] = true
				continue
			}
			folioAmcName := resolveFolioAmcName(f, schemes, amcMap)
			if f.AmcName == "" {
				logger.LogInfo("[bulk] folio amc_name was empty, using: %s", folioAmcName)
			}
			var existing string
			if f.FolioNumber != "" && f.EntityName != "" {
				logger.LogInfo("[bulk] folio checking existing: folio=%s, entity=%s, amc=%s", f.FolioNumber, f.EntityName, folioAmcName)
				_ = tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number=$1 AND TRIM(entity_name)=TRIM($2) AND COALESCE(is_deleted,false)=false LIMIT 1`, f.FolioNumber, f.EntityName).Scan(&existing)
			}
			if existing != "" {
				logger.LogInfo("[bulk] folio found existing: %s", existing)
				folioMap[f.FolioNumber] = existing
				// Create mapping entry
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, folio_id, folio_number) VALUES ($1, $2, $3, $4)`, batchID, f.EntityName, existing, f.FolioNumber); err != nil {
					logger.LogError(constants.ErrBulkFolioMappingInsertFailed, err)
					api.RespondWithError(w, 500, constants.ErrBulkFolioMappingFailed+err.Error())
					return
				}
				if f.Enriched {
					enrichedCounts["folio"]++
					enrichedFolios[f.FolioNumber] = true
					continue
				}
				continue
			}
			var folioID string
			if err := tx.QueryRow(ctx, `INSERT INTO investment.masterfolio (entity_name, amc_name, folio_number, first_holder_name, default_subscription_account, default_redemption_account, status, source) VALUES ($1,$2,$3,$4,$5,$6,$7,'Manual') RETURNING folio_id`, f.EntityName, folioAmcName, f.FolioNumber, f.FirstHolderName, defaultIfEmpty(f.DefaultSubscriptionAccount, "SYSTEM"), defaultIfEmpty(f.DefaultRedemptionAccount, "SYSTEM"), defaultIfEmpty(f.Status, "Active")).Scan(&folioID); err != nil {
				logger.LogError("[bulk] insert folio failed: %v", err)
				api.RespondWithError(w, 500, "insert folio failed: "+err.Error())
				return
			}
			// Create mapping entry for new folio
			if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, folio_id, folio_number) VALUES ($1, $2, $3, $4)`, batchID, f.EntityName, folioID, f.FolioNumber); err != nil {
				logger.LogError(constants.ErrBulkFolioMappingInsertFailed, err)
				api.RespondWithError(w, 500, constants.ErrBulkFolioMappingFailed+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, requested_by, requested_at, requested_ip) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3)`, folioID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
				logger.LogError("[bulk] audit folio failed: %v", err)
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
							logger.LogError("[bulk] folio-scheme map failed: %v", err)
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
		for i, s := range schemes {
			if isBlankScheme(s) {
				logger.LogInfo("[bulk] scheme[%d] skipped: blank placeholder row", i)
				continue
			}
			if msg := validateSchemeRequired(i, s); msg != "" {
				api.RespondWithError(w, http.StatusBadRequest, msg)
				return
			}
			logger.LogInfo("[bulk] processing scheme: %+v", s)
			var existing string

			// For enriched entities, look up by name and amc first
			if s.Enriched && s.SchemeName != "" {
				logger.LogInfo("[bulk] scheme is enriched, looking up by name: %s (amc: %s)", s.SchemeName, s.AmcName)
				if s.AmcName != "" {
					_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE scheme_name=$1 AND amc_name=$2 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.SchemeName, s.AmcName).Scan(&existing)
				} else {
					_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE scheme_name=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.SchemeName).Scan(&existing)
				}
				if existing != "" {
					logger.LogInfo("[bulk] scheme found enriched entity: %s", existing)
					schemeMap[defaultIfEmpty(s.InternalSchemeCode, s.SchemeName)] = existing
					// Create mapping entry for enriched scheme
					if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, scheme_id, scheme_name) VALUES ($1, $2, $3)`, batchID, existing, s.SchemeName); err != nil {
						logger.LogError(constants.ErrBulkSchemeMappingInsertFailed, err)
						api.RespondWithError(w, 500, constants.ErrBulkSchemeMappingFailed+err.Error())
						return
					}
					continue
				}
			}

			// lookup by internal code, isin, or name
			if s.InternalSchemeCode != "" {
				logger.LogInfo("[bulk] scheme checking existing by internal_code: %s", s.InternalSchemeCode)
				_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE internal_scheme_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.InternalSchemeCode).Scan(&existing)
				enrichedSchemes[defaultIfEmpty(s.InternalSchemeCode, s.SchemeName)] = true
			}
			if existing == "" && s.ISIN != "" {
				logger.LogInfo("[bulk] scheme checking existing by isin: %s", s.ISIN)
				_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE isin=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.ISIN).Scan(&existing)
			}
			if existing == "" && s.SchemeName != "" {
				logger.LogInfo("[bulk] scheme checking existing by name: %s", s.SchemeName)
				_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE scheme_name=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, s.SchemeName).Scan(&existing)
			}
			if existing != "" {
				logger.LogInfo("[bulk] scheme found existing: %s", existing)
				schemeMap[defaultIfEmpty(s.InternalSchemeCode, s.SchemeName)] = existing
				// Create mapping entry for existing scheme
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, scheme_id, scheme_name) VALUES ($1, $2, $3)`, batchID, existing, s.SchemeName); err != nil {
					logger.LogError(constants.ErrBulkSchemeMappingInsertFailed, err)
					api.RespondWithError(w, 500, constants.ErrBulkSchemeMappingFailed+err.Error())
					return
				}
				if s.Enriched {
					enrichedCounts["scheme"]++
					continue
				}
				continue
			}
			var schemeID string
			enrichedSchemes[defaultIfEmpty(s.InternalSchemeCode, s.SchemeName)] = true
			if err := tx.QueryRow(ctx, `INSERT INTO investment.masterscheme (scheme_name, isin, amc_name, internal_scheme_code, internal_risk_rating, erp_gl_account, status, method, source) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'Manual') RETURNING scheme_id`,
				s.SchemeName,
				s.ISIN,
				s.AmcName,
				s.InternalSchemeCode,
				defaultIfEmpty(s.InternalRiskRating, "Medium"),
				defaultIfEmpty(s.ErpGlAccount, "GL000"),
				defaultIfEmpty(s.Status, "Active"),
				defaultIfEmpty(strings.ToUpper(strings.TrimSpace(s.Method)), "FIFO")).Scan(&schemeID); err != nil {
				logger.LogError("[bulk] insert scheme failed: %v", err)
				api.RespondWithError(w, 500, "insert scheme failed: "+err.Error())
				return
			}
			// Create mapping entry for new scheme
			if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, scheme_id, scheme_name) VALUES ($1, $2, $3)`, batchID, schemeID, s.SchemeName); err != nil {
				logger.LogError(constants.ErrBulkSchemeMappingInsertFailed, err)
				api.RespondWithError(w, 500, constants.ErrBulkSchemeMappingFailed+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, requested_by, requested_at, requested_ip) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3)`, schemeID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
				logger.LogError("[bulk] audit scheme failed: %v", err)
				api.RespondWithError(w, 500, "audit scheme failed: "+err.Error())
				return
			}
			schemeMap[s.InternalSchemeCode] = schemeID
			counts["scheme"]++
		}

		// Link schemes from this batch to folios (new or existing) when AMC matches.
		for folioNum, folioID := range folioMap {
			var folioAmc sql.NullString
			_ = tx.QueryRow(ctx, `SELECT amc_name FROM investment.masterfolio WHERE folio_id=$1`, folioID).Scan(&folioAmc)
			for _, s := range schemes {
				if isBlankScheme(s) {
					continue
				}
				key := defaultIfEmpty(s.InternalSchemeCode, s.SchemeName)
				sid := schemeMap[key]
				if sid == "" && s.SchemeName != "" {
					sid = schemeMap[s.SchemeName]
				}
				if sid == "" {
					continue
				}
				schemeAmc := strings.TrimSpace(s.AmcName)
				if folioAmc.Valid && schemeAmc != "" && !strings.EqualFold(folioAmc.String, schemeAmc) {
					continue
				}
				if _, err := tx.Exec(ctx, `INSERT INTO investment.folioschememapping (folio_id, scheme_id, status) VALUES ($1,$2,'Active') ON CONFLICT DO NOTHING`, folioID, sid); err != nil {
					logger.LogError("[bulk] folio-scheme map failed folio=%s scheme=%s: %v", folioNum, key, err)
					api.RespondWithError(w, 500, "folio-scheme map failed: "+err.Error())
					return
				}
			}
		}

		// Insert onboard_mapping for all created or enriched entries
		for k, v := range amcMap {
			isEnriched := enrichedAMCs[k]
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'AMC',$3,$4)`, batchID, v, k, isEnriched); err != nil {
				logger.LogError("[bulk] onboard mapping amc failed: %v", err)
				api.RespondWithError(w, 500, constants.ErrBulkOnboardMappingFailed+err.Error())
				return
			}
		}
		for k, v := range schemeMap {
			isEnriched := enrichedSchemes[k]
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'SCHEME',$3,$4)`, batchID, v, k, isEnriched); err != nil {
				logger.LogError("[bulk] onboard mapping scheme failed: %v", err)
				api.RespondWithError(w, 500, constants.ErrBulkOnboardMappingFailed+err.Error())
				return
			}
		}
		for k, v := range dpMap {
			isEnriched := enrichedDPs[k]
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'DP',$3,$4)`, batchID, v, k, isEnriched); err != nil {
				logger.LogError("[bulk] onboard mapping dp failed: %v", err)
				api.RespondWithError(w, 500, constants.ErrBulkOnboardMappingFailed+err.Error())
				return
			}
		}
		for k, v := range dematMap {
			isEnriched := enrichedDemats[k]
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'DEMAT',$3,$4)`, batchID, v, k, isEnriched); err != nil {
				logger.LogError("[bulk] onboard mapping demat failed: %v", err)
				api.RespondWithError(w, 500, constants.ErrBulkOnboardMappingFailed+err.Error())
				return
			}
		}
		for k, v := range folioMap {
			isEnriched := enrichedFolios[k]
			if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_mapping (batch_id, reference_id, reference_type, reference_name, enriched) VALUES ($1,$2,'FOLIO',$3,$4)`, batchID, v, k, isEnriched); err != nil {
				logger.LogError("[bulk] onboard mapping folio failed: %v", err)
				api.RespondWithError(w, 500, constants.ErrBulkOnboardMappingFailed+err.Error())
				return
			}
		}
		logger.LogInfo("[bulk] onboard_mapping inserted")

		// Insert portfolio_onboarding_map rows for all entities
		logger.LogInfo("[bulk] populating portfolio_onboarding_map for all entities")

		// Insert for AMCs
		for amcKey, amcID := range amcMap {
			var entityName, amcName sql.NullString
			if err := tx.QueryRow(ctx, `SELECT amc_name, amc_name FROM investment.masteramc WHERE amc_id=$1`, amcID).Scan(&entityName, &amcName); err == nil {
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, amc_id, created_at) VALUES ($1,$2,$3,now())`, batchID, entityName.String, amcID); err != nil {
					logger.LogError("[bulk] portfolio_onboarding_map AMC insert failed: %v", err)
				} else {
					logger.LogInfo("[bulk] inserted portfolio_onboarding_map for AMC: %s", amcKey)
				}
			}
		}

		// Insert for DPs
		for dpKey, dpID := range dpMap {
			var entityName, dpName sql.NullString
			if err := tx.QueryRow(ctx, `SELECT dp_name, dp_name FROM investment.masterdepositoryparticipant WHERE dp_id=$1`, dpID).Scan(&entityName, &dpName); err == nil {
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, created_at) VALUES ($1,$2,now())`, batchID, entityName.String); err != nil {
					logger.LogError("[bulk] portfolio_onboarding_map DP insert failed: %v", err)
				} else {
					logger.LogInfo("[bulk] inserted portfolio_onboarding_map for DP: %s", dpKey)
				}
			}
		}

		// Insert for Demats
		for dematKey, dematID := range dematMap {
			var entityName, dematAccNum sql.NullString
			if err := tx.QueryRow(ctx, `SELECT entity_name, demat_account_number FROM investment.masterdemataccount WHERE demat_id=$1`, dematID).Scan(&entityName, &dematAccNum); err == nil {
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, demat_id, created_at) VALUES ($1,$2,$3,now())`, batchID, entityName.String, dematID); err != nil {
					logger.LogError("[bulk] portfolio_onboarding_map Demat insert failed: %v", err)
				} else {
					logger.LogInfo("[bulk] inserted portfolio_onboarding_map for Demat: %s", dematKey)
				}
			}
		}

		// Insert for Schemes
		for schemeKey, schemeID := range schemeMap {
			var entityName, schemeName sql.NullString
			if err := tx.QueryRow(ctx, `SELECT amc_name, scheme_name FROM investment.masterscheme WHERE scheme_id=$1`, schemeID).Scan(&entityName, &schemeName); err == nil {
				if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, scheme_id, scheme_name, created_at) VALUES ($1,$2,$3,$4,now())`, batchID, entityName.String, schemeID, schemeName.String); err != nil {
					logger.LogError("[bulk] portfolio_onboarding_map Scheme insert failed: %v", err)
				} else {
					logger.LogInfo("[bulk] inserted portfolio_onboarding_map for Scheme: %s", schemeKey)
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
						logger.LogError("[bulk] portfolio_onboarding_map Folio+Scheme insert failed: %v", err)
					} else {
						logger.LogInfo("[bulk] inserted portfolio_onboarding_map for Folio+Scheme: %s+%s", folioKey, sname)
						schemeFound = true
					}
				}
				schemeRows.Close()

				// If no schemes found, insert folio-only record
				if !schemeFound {
					if _, err := tx.Exec(ctx, `INSERT INTO investment.portfolio_onboarding_map (batch_id, entity_name, folio_id, folio_number, amc_id, created_at) VALUES ($1,$2,$3,$4,$5,now())`,
						batchID, entityName.String, folioID, folioNumber.String, nullableString(relatedAmcID.String)); err != nil {
						logger.LogError("[bulk] portfolio_onboarding_map Folio insert failed: %v", err)
					} else {
						logger.LogInfo("[bulk] inserted portfolio_onboarding_map for Folio: %s", folioKey)
					}
				}
			}
		}
		logger.LogInfo("[bulk] portfolio_onboarding_map populated for all entity types")

		// Parse transactions CSV if present - check both "files" and "transactions" keys
		var txRows []TxCSVRow
		var fhArr []*multipart.FileHeader
		if files := r.MultipartForm.File["files"]; len(files) > 0 {
			fhArr = files
			logger.LogInfo("[bulk] found CSV under 'files' key")
		} else if txFiles := r.MultipartForm.File["transactions"]; len(txFiles) > 0 {
			fhArr = txFiles
			logger.LogInfo("[bulk] found CSV under 'transactions' key")
		}

		if len(fhArr) > 0 {
			f, err := fhArr[0].Open()
			if err != nil {
				logger.LogError("[bulk] open transactions file: %v", err)
				api.RespondWithError(w, 400, "open transactions file: "+err.Error())
				return
			}
			txRows, err = parseTransactionsCSV(f)
			f.Close()
			if err != nil {
				logger.LogError("[bulk] parse transactions csv: %v", err)
				api.RespondWithError(w, 400, "parse transactions: "+err.Error())
				return
			}
			logger.LogInfo("[bulk] parsed %d transaction rows", len(txRows))

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

			// Insert transactions with strict validation
			for rowNum, tr := range txRows {
				// ── Amount validations ───────────────────────────────────────────────
				if tr.Amount < 0 {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf(
						"Transaction row %d: amount cannot be negative (%.2f). Use TransactionType to indicate direction (Purchase/Sell).",
						rowNum+1, tr.Amount))
					return
				}
				if tr.Nav > 0 && tr.Units > 0 {
					expected := tr.Nav * tr.Units
					if math.Abs(tr.Amount-expected) > expected*0.01 { // 1% tolerance for rounding
						api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf(
							"Transaction row %d: amount (%.2f) must equal NAV (%.4f) × Units (%.4f) = %.2f (difference exceeds 1%% rounding tolerance).",
							rowNum+1, tr.Amount, tr.Nav, tr.Units, expected))
						return
					}
				}

				// Log enrichment lookups (informational only — resolution happens below)
				if enrichedScheme, exists := enrichedSchemesByCode[tr.SchemeInternalCode]; exists {
					logger.LogInfo("[bulk] tx row %d: scheme '%s' found in batch as '%s'", rowNum+1, tr.SchemeInternalCode, enrichedScheme.SchemeName)
				}
				if tr.FolioNumber != "" {
					if enrichedFolio, exists := enrichedFoliosByNumber[tr.FolioNumber]; exists {
						logger.LogInfo("[bulk] tx row %d: folio '%s' found in batch for entity '%s'", rowNum+1, tr.FolioNumber, enrichedFolio.EntityName)
					}
				}
				if tr.DematAccNumber != "" {
					if enrichedDemat, exists := enrichedDematsByNumber[tr.DematAccNumber]; exists {
						logger.LogInfo("[bulk] tx row %d: demat '%s' found in batch for entity '%s'", rowNum+1, tr.DematAccNumber, enrichedDemat.EntityName)
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
						_ = tx.QueryRow(ctx, `SELECT entity_name FROM investment.masterfolio WHERE folio_number=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, tr.FolioNumber).Scan(&entityName)
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
						_ = tx.QueryRow(ctx, `SELECT entity_name FROM investment.masterdemataccount WHERE demat_account_number=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, tr.DematAccNumber).Scan(&entityName)
					}
				}

				// Normalise transaction type
				rawType := strings.TrimSpace(tr.TransactionType)
				normType := "Purchase"
				if strings.EqualFold(rawType, "purchase") || strings.EqualFold(rawType, "buy") || strings.EqualFold(rawType, "subscription") {
					normType = "Purchase"
				} else if strings.EqualFold(rawType, "sell") || strings.EqualFold(rawType, "redemption") || strings.EqualFold(rawType, "redeem") {
					normType = "Redemption"
				} else {
					// Fallback to titlecase if unknown
					normType = strings.Title(strings.ToLower(rawType))
				}
				tr.TransactionType = normType

				// Resolve scheme_id (prioritize from enriched data, fallback to DB)
				var schemeID string
				if v, ok := schemeMap[tr.SchemeInternalCode]; ok {
					schemeID = v
					logger.LogInfo("[bulk] transaction resolved scheme_id from schemeMap: %s -> %s", tr.SchemeInternalCode, schemeID)
				} else {
					_ = tx.QueryRow(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE internal_scheme_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, tr.SchemeInternalCode).Scan(&schemeID)
					if schemeID != "" {
						logger.LogInfo("[bulk] transaction resolved scheme_id from DB: %s -> %s", tr.SchemeInternalCode, schemeID)
					} else {
						logger.LogInfo("[bulk] WARNING: transaction could not resolve scheme_id for internal_code: %s", tr.SchemeInternalCode)
					}
				}

				// Resolve folio_id if folio_number provided (prioritize from enriched data, fallback to DB)
				var folioID string
				if tr.FolioNumber != "" {
					if v, ok := folioMap[tr.FolioNumber]; ok {
						folioID = v
					} else {
						_ = tx.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, tr.FolioNumber).Scan(&folioID)
					}
				}

				// Resolve demat_id if demat_acc_number provided (prioritize from enriched data, fallback to DB)
				var dematID string
				if tr.DematAccNumber != "" {
					if v, ok := dematMap[tr.DematAccNumber]; ok {
						dematID = v
					} else {
						_ = tx.QueryRow(ctx, `SELECT demat_id FROM investment.masterdemataccount WHERE demat_account_number=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, tr.DematAccNumber).Scan(&dematID)
					}
				}

				// ── Strict cross-reference checks ───────────────────────────────────
				// scheme_internal_code MUST resolve to a scheme in this batch or in masterscheme
				if tr.SchemeInternalCode != "" && schemeID == "" {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf(
						"Transaction row %d: scheme_internal_code '%s' was not found in this batch or in approved schemes. Transactions must reference schemes included in this upload.",
						rowNum+1, tr.SchemeInternalCode))
					return
				}
				// folio_number MUST resolve when provided
				if tr.FolioNumber != "" && folioID == "" {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf(
						"Transaction row %d: folio_number '%s' was not found in this batch or in master folios.",
						rowNum+1, tr.FolioNumber))
					return
				}
				// demat_account_number MUST resolve when provided
				if tr.DematAccNumber != "" && dematID == "" {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf(
						"Transaction row %d: demat_account_number '%s' was not found in this batch or in master demat accounts.",
						rowNum+1, tr.DematAccNumber))
					return
				}

				// Insert transaction with resolved IDs and entity_name
				// Note: Store all values as POSITIVE. Transaction type ('Purchase', 'Sell', etc.)
				// determines direction in snapshot calculations.
				if _, err := tx.Exec(ctx, `INSERT INTO investment.onboard_transaction (batch_id, transaction_date, transaction_type, scheme_internal_code, folio_number, demat_acc_number, amount, units, nav, scheme_id, folio_id, demat_id, entity_name, created_at, approval_status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now(),'PENDING')`, batchID, tr.TransactionDate, tr.TransactionType, tr.SchemeInternalCode, nullableString(tr.FolioNumber), nullableString(tr.DematAccNumber), tr.Amount, tr.Units, tr.Nav, nullableString(schemeID), nullableString(folioID), nullableString(dematID), nullableString(entityName)); err != nil {
					logger.LogError("[bulk] insert onboard_transaction failed: %v", err)
					api.RespondWithError(w, 500, "insert transaction failed: "+err.Error())
					return
				}
				counts["transactions"]++
			}
			logger.LogInfo("[bulk] inserted %d transactions with entity_name populated (validated against enriched data where available)", counts["transactions"])

			// STRICT VALIDATION: Ensure at least one transaction was inserted
			if counts["transactions"] == 0 {
				logger.LogInfo("[bulk] STRICT: no transactions inserted, rolling back batch %s", batchID)
				api.RespondWithError(w, 400, "No transactions were inserted. Upload must contain valid transactions.")
				return
			}
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
        COALESCE(ot.entity_name, mf.entity_name, md.entity_name) AS entity_name,
        ot.folio_number,
        ot.demat_acc_number,
        ot.folio_id,
        ot.demat_id,
        -- Prioritize scheme_id from transaction, then resolve via internal_code or folio mapping
        COALESCE(ot.scheme_id, resolved_scheme.scheme_id::text) AS scheme_id,
        resolved_scheme.scheme_name,
        resolved_scheme.isin
    FROM investment.onboard_transaction ot
    LEFT JOIN investment.masterfolio mf 
        ON mf.folio_id = ot.folio_id
    LEFT JOIN investment.masterdemataccount md
        ON md.demat_id = ot.demat_id
    LEFT JOIN LATERAL (
        SELECT scheme_id, scheme_name, isin
        FROM investment.masterscheme
        WHERE scheme_id::text = ot.scheme_id 
           OR internal_scheme_code = ot.scheme_internal_code
        ORDER BY created_at DESC 
        LIMIT 1
    ) AS resolved_scheme ON true
    WHERE ot.batch_id = $1
),
transaction_summary AS (
    SELECT
        entity_name,
        folio_number,
        demat_acc_number,
        folio_id,
        demat_id,
        scheme_id,
        scheme_name,
        isin,
` + portfolio.TransactionSummaryMetrics + `
    FROM scheme_resolved
    GROUP BY entity_name, folio_number, demat_acc_number, folio_id, demat_id, scheme_id, scheme_name, isin
)
INSERT INTO investment.portfolio_snapshot (
  batch_id,
  entity_name,
  folio_number,
  demat_acc_number,
  folio_id,
  demat_id,
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
  ts.folio_id,
  ts.demat_id,
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
` + schemejoin.NavLateralJoin("ts", "") + `
WHERE ts.total_units > 0;
`

			if _, err := tx.Exec(ctx, aggQ, batchID); err != nil {
				logger.LogError("[bulk] insert snapshot failed: %v", err)
				api.RespondWithError(w, 500, "snapshot insert failed: "+err.Error())
				return
			}
			// count snapshot rows
			var snapshotCnt int64
			if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM investment.portfolio_snapshot WHERE batch_id=$1`, batchID).Scan(&snapshotCnt); err != nil {
				logger.LogError("[bulk] snapshot count failed: %v", err)
				api.RespondWithError(w, 500, "snapshot count failed: "+err.Error())
				return
			}
			counts["snapshot"] = snapshotCnt
			logger.LogInfo("[bulk] inserted %d snapshot rows", counts["snapshot"])
		} else {
			logger.LogInfo("[bulk] no transactions found, skipping snapshot creation")
		}

		// finalize batch update totals & status
		if _, err := tx.Exec(ctx, `UPDATE investment.onboard_batch SET total_records = (SELECT COUNT(*) FROM investment.onboard_transaction WHERE batch_id=$1), status='COMPLETED', completed_at=now() WHERE batch_id=$1`, batchID); err != nil {
			logger.LogError("[bulk] finalize batch failed: %v", err)
			api.RespondWithError(w, 500, "finalize batch failed: "+err.Error())
			return
		}

		// commit
		if err := tx.Commit(ctx); err != nil {
			logger.LogError("[bulk] commit failed: %v", err)
			api.RespondWithError(w, 500, constants.ErrCommitFailed+err.Error())
			return
		}
		tx = nil
		logger.LogInfo("[bulk] committed batch %s", batchID)

		// Fire notification asynchronously
		batchIDCopy := batchID
		userEmailCopy := userEmail
		poolRef := pgxPool
		go BuildOnboardUploadNotifPayload(context.Background(), poolRef, batchIDCopy, userEmailCopy)

		logger.LogInfo("[bulk] final counts: %+v", counts)
		logger.LogInfo("[bulk] enriched counts: %+v", enrichedCounts)
		logger.LogInfo("[bulk] final counts: %+v", counts)
		logger.LogInfo("[bulk] enriched counts: %+v", enrichedCounts)

		res := UploadResponse{Success: true, BatchID: batchID, Counts: counts, EnrichedCounts: enrichedCounts, Message: "bulk onboard completed"}
		logger.LogInfo("[bulk] sending response: %+v", res)
		api.RespondWithPayload(w, true, "", res)
	}
}

func GetOnboardDownloadURL(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string `json:"user_id"`
			BatchID string `json:"batch_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
			return
		}
		if strings.TrimSpace(req.BatchID) == "" {
			api.RespondWithResult(w, false, constants.BatchIDsRequired)
			return
		}

		var uploadS3Key sql.NullString
		if err := pgxPool.QueryRow(r.Context(), `
			SELECT upload_s3_key
			FROM investment.onboard_batch
			WHERE batch_id = $1
		`, req.BatchID).Scan(&uploadS3Key); err != nil {
			api.RespondWithResult(w, false, constants.ErrFileNotFound)
			return
		}

		key := strings.TrimSpace(uploadS3Key.String)
		if !uploadS3Key.Valid || key == "" {
			api.RespondWithResult(w, false, "no file available")
			return
		}

		downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), key, 15*time.Minute)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to generate download url: "+err.Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data": map[string]interface{}{
				"batch_id":     req.BatchID,
				"download_url": downloadURL,
			},
		})
	}
}

// func GetOnboardDownloadURL(pgxPool *pgxpool.Pool) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		var req struct {
// 			UserID  string `json:"user_id"`
// 			BatchID string `json:"batch_id"`
// 		}
// 		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
// 			return
// 		}
// 		if strings.TrimSpace(req.BatchID) == "" {
// 			api.RespondWithResult(w, false, constants.BatchIDsRequired)
// 			return
// 		}

// 		var uploadS3Key sql.NullString
// 		if err := pgxPool.QueryRow(r.Context(), `
// 			SELECT upload_s3_key
// 			FROM investment.onboard_batch
// 			WHERE batch_id = $1
// 		`, req.BatchID).Scan(&uploadS3Key); err != nil {
// 			api.RespondWithResult(w, false, constants.ErrFileNotFound )
// 			return
// 		}

// 		key := strings.TrimSpace(uploadS3Key.String)
// 		if !uploadS3Key.Valid || key == "" {
// 			api.RespondWithResult(w, false, "no file available")
// 			return
// 		}

// 		downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), key, 15*time.Minute)
// 		if err != nil {
// 			api.RespondWithResult(w, false, "Failed to generate download url: "+err.Error())
// 			return
// 		}

// 		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
// 		_ = json.NewEncoder(w).Encode(map[string]interface{}{
// 			constants.ValueSuccess: true,
// 			"data": map[string]interface{}{
// 				"batch_id":     req.BatchID,
// 				"download_url": downloadURL,
// 			},
// 		})
// 	}
// }

func GetOnboardBulkDownloadURL(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			BatchIDs []string `json:"batch_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, "Invalid JSON: "+err.Error())
			return
		}
		if len(req.BatchIDs) == 0 {
			api.RespondWithResult(w, false, "batch_ids required")
			return
		}

		files := make([]map[string]string, 0, len(req.BatchIDs))
		failedIDs := make([]string, 0)
		for _, rawID := range req.BatchIDs {
			batchID := strings.TrimSpace(rawID)
			if batchID == "" {
				continue
			}

			var uploadS3Key sql.NullString
			if err := pgxPool.QueryRow(r.Context(), `
				SELECT upload_s3_key
				FROM investment.onboard_batch
				WHERE batch_id = $1
			`, batchID).Scan(&uploadS3Key); err != nil {
				failedIDs = append(failedIDs, batchID)
				continue
			}

			key := strings.TrimSpace(uploadS3Key.String)
			if !uploadS3Key.Valid || key == "" {
				failedIDs = append(failedIDs, batchID)
				continue
			}

			downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), key, 15*time.Minute)
			if err != nil {
				failedIDs = append(failedIDs, batchID)
				continue
			}
			files = append(files, map[string]string{
				"batch_id":     batchID,
				"download_url": downloadURL,
			})
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: len(files) > 0,
			"data": map[string]interface{}{
				"files":      files,
				"failed_ids": failedIDs,
			},
		})
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
		constants.DateFormat,
		"2006/01/02",
		"2006.01.02",
		time.RFC3339,
		constants.DateTimeFormat,
		constants.DateFormatISO,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",

		// DD-MM-YYYY formats
		constants.DateFormatAlt,
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
		constants.DateFormatDash,
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
			return t.Format(constants.DateFormat), nil
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
							return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).Format(constants.DateFormat), nil
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
				return t.Format(constants.DateFormat), nil
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
		contentType := r.Header.Get(constants.ContentTypeText)
		if strings.Contains(contentType, constants.ContentTypeJSON) {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				api.RespondWithError(w, 400, constants.ErrInvalidJSONPrefix+err.Error())
				return
			}
		} else {
			_ = r.ParseForm()
			req.BatchID = r.FormValue("batch_id")
		}

		if req.BatchID == "" {
			api.RespondWithError(w, 400, constants.BatchIDsRequired)
			return
		}

		batchID := req.BatchID
		logger.LogInfo("[refresh] refreshing portfolio snapshot for batch %s", batchID)

		// Delete existing snapshots for this batch
		if _, err := pgxPool.Exec(ctx, `DELETE FROM investment.portfolio_snapshot WHERE batch_id = $1`, batchID); err != nil {
			logger.LogError("[refresh] delete existing snapshots failed: %v", err)
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
        ot.folio_id,
        ot.demat_id,
        COALESCE(ms.scheme_id::text, ot.scheme_id) AS scheme_id,
        COALESCE(ms.scheme_name, '') AS scheme_name,
        COALESCE(ms.isin, '') AS isin,
        COALESCE(ms.amfi_scheme_code, '') AS amfi_scheme_code
    FROM investment.onboard_transaction ot
    LEFT JOIN investment.masterfolio mf 
        ON mf.folio_id = ot.folio_id
    LEFT JOIN investment.masterdemataccount md
        ON md.demat_id = ot.demat_id
    LEFT JOIN investment.masterscheme ms ON (
        COALESCE(ms.is_deleted, false) = false
        AND (
            (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id))
            OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code))
            OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id))
        )
    )
    WHERE ot.batch_id = $1
),
transaction_summary AS (
    SELECT
        entity_name,
        folio_number,
        demat_acc_number,
        folio_id,
        demat_id,
        scheme_id,
        scheme_name,
        isin,
        MAX(amfi_scheme_code) AS amfi_scheme_code,
` + portfolio.TransactionSummaryMetrics + `
    FROM scheme_resolved
    GROUP BY entity_name, folio_number, demat_acc_number, folio_id, demat_id, scheme_id, scheme_name, isin
)
INSERT INTO investment.portfolio_snapshot (
  batch_id,
  entity_name,
  folio_number,
  demat_acc_number,
  folio_id,
  demat_id,
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
  ts.folio_id,
  ts.demat_id,
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
` + schemejoin.NavLateralJoin("ts", "ts.amfi_scheme_code") + `
WHERE ts.total_units > 0;
`

		if _, err := pgxPool.Exec(ctx, recalcQ, batchID); err != nil {
			logger.LogError("[refresh] recalculate snapshot failed: %v", err)
			api.RespondWithError(w, 500, "recalculate snapshot failed: "+err.Error())
			return
		}

		// Count new snapshots
		var snapshotCnt int64
		if err := pgxPool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.portfolio_snapshot WHERE batch_id=$1`, batchID).Scan(&snapshotCnt); err != nil {
			logger.LogError("[refresh] count snapshots failed: %v", err)
			api.RespondWithError(w, 500, "count snapshots failed: "+err.Error())
			return
		}

		logger.LogInfo("[refresh] refreshed %d snapshot rows for batch %s", snapshotCnt, batchID)
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
		contentType := r.Header.Get(constants.ContentTypeText)
		if strings.Contains(contentType, constants.ContentTypeJSON) {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				api.RespondWithError(w, 400, constants.ErrInvalidJSONPrefix+err.Error())
				return
			}
		} else {
			_ = r.ParseForm()
			req.BatchID = r.FormValue("batch_id")
		}

		if req.BatchID == "" {
			api.RespondWithError(w, 400, constants.BatchIDsRequired)
			return
		}

		batchID := req.BatchID

		summaryQ := `
        WITH tx_agg AS (
            SELECT 
                ot.folio_number,
                COALESCE(ms.scheme_id::text, ot.scheme_id) AS scheme_id,
                COALESCE(ms.scheme_name, '') AS scheme_name,
                COALESCE(ms.isin, '') AS isin,
                SUM(ot.amount) AS total_investment,
                SUM(ot.units) AS total_units,
                MAX(COALESCE(ms.amfi_scheme_code, '')) AS amfi_scheme_code
            FROM investment.onboard_transaction ot
            LEFT JOIN investment.masterfolio mf ON mf.folio_number = ot.folio_number
            LEFT JOIN investment.masterscheme ms ON (` + schemejoin.JoinOnboardTx + `)
            WHERE ot.batch_id = $1
            GROUP BY ot.folio_number, COALESCE(ms.scheme_id::text, ot.scheme_id), COALESCE(ms.scheme_name, ''), COALESCE(ms.isin, '')
        )
        SELECT 
            COALESCE(SUM(total_investment),0) AS total_investment,
            COALESCE(SUM(total_units * COALESCE(ln.nav_value,0)),0) AS current_value
        FROM tx_agg ta
        ` + schemejoin.NavLateralJoin("ta", constants.ColTAAmfiSchemeCode) + `;
        `

		var totalInv, currVal float64
		if err := pgxPool.QueryRow(ctx, summaryQ, batchID).Scan(&totalInv, &currVal); err != nil {
			logger.LogError("[dash] summary query failed: %v", err)
			api.RespondWithError(w, 500, "summary query failed: "+err.Error())
			return
		}

		gain := currVal - totalInv
		gainPct := 0.0
		if totalInv != 0 {
			gainPct = (gain / totalInv) * 100
		}

		allocQ := `
        WITH tx_agg AS (
            SELECT 
                ot.folio_number,
                COALESCE(ms.scheme_id::text, ot.scheme_id) AS scheme_id,
                COALESCE(ms.scheme_name, '') AS scheme_name,
                COALESCE(ms.isin, '') AS isin,
                MAX(COALESCE(ms.amfi_scheme_code, '')) AS amfi_scheme_code,
                SUM(ot.units) AS total_units
            FROM investment.onboard_transaction ot
            LEFT JOIN investment.masterfolio mf ON mf.folio_id = ot.folio_id
            LEFT JOIN investment.masterscheme ms ON (` + schemejoin.JoinOnboardTx + `)
            WHERE ot.batch_id = $1
            GROUP BY ot.folio_number, COALESCE(ms.scheme_id::text, ot.scheme_id), COALESCE(ms.scheme_name, ''), COALESCE(ms.isin, '')
        )
        SELECT 
            COALESCE(ln.raw_category_header, 'Uncategorized') AS category,
            SUM(ta.total_units * COALESCE(ln.nav_value,0)) AS value
        FROM tx_agg ta
        ` + schemejoin.NavLateralJoin("ta", constants.ColTAAmfiSchemeCode) + `
        GROUP BY COALESCE(ln.raw_category_header, 'Uncategorized');
        `

		allocRows, err := pgxPool.Query(ctx, allocQ, batchID)
		if err != nil {
			logger.LogError("[dash] allocation query failed: %v", err)
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
        WITH tx_agg AS (
            SELECT 
                ot.folio_number,
                COALESCE(ms.scheme_id::text, ot.scheme_id) AS scheme_id,
                COALESCE(ms.scheme_name, '') AS scheme_name,
                COALESCE(ms.isin, '') AS isin,
                MAX(COALESCE(ms.amfi_scheme_code, '')) AS amfi_scheme_code,
                SUM(ot.units) AS total_units
            FROM investment.onboard_transaction ot
            LEFT JOIN investment.masterfolio mf ON mf.folio_id = ot.folio_id
            LEFT JOIN investment.masterscheme ms ON (` + schemejoin.JoinOnboardTx + `)
            WHERE ot.batch_id = $1
            GROUP BY ot.folio_number, COALESCE(ms.scheme_id::text, ot.scheme_id), COALESCE(ms.scheme_name, ''), COALESCE(ms.isin, '')
        )
        SELECT 
            ta.scheme_name,
            ta.folio_number,
            ta.total_units,
            COALESCE(ln.nav_value,0) AS current_nav,
            ta.total_units * COALESCE(ln.nav_value,0) AS current_value
        FROM tx_agg ta
        ` + schemejoin.NavLateralJoin("ta", constants.ColTAAmfiSchemeCode) + `
        ORDER BY current_value DESC;
        `

		rows, err := pgxPool.Query(ctx, holdingsQ, batchID)
		if err != nil {
			logger.LogError("[dash] holdings query failed: %v", err)
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
