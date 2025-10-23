package exposures

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	api "CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)


type ExposureResponse struct {
	ExposureHeaderID    string     `json:"exposure_header_id"`
	CompanyCode         string     `json:"company_code"`
	Entity              string     `json:"entity"`
	DocumentID          string     `json:"document_id"`
	DocumentDate        *time.Time `json:"document_date"`
	PostingDate         *time.Time `json:"posting_date"`
	ValueDate           *time.Time `json:"value_date"`
	CounterpartyCode    string     `json:"counterparty_code"`
	CounterpartyName    string     `json:"counterparty_name"`
	Currency            string     `json:"currency"`
	TotalOriginalAmount float64    `json:"total_original_amount"`
	TotalOpenAmount     float64    `json:"total_open_amount"`
	AgingDays           *int       `json:"aging_days"`
	Status              string     `json:"status"`
	ApprovalStatus      string     `json:"approval_status"`
	ApprovedBy          string     `json:"approved_by"`
	ApprovedAt          *time.Time `json:"approved_at"`
	RejectedBy          string     `json:"rejected_by"`
	RejectedAt          *time.Time `json:"rejected_at"`
	RejectionComment    string     `json:"rejection_comment"`
	DeleteComment       string     `json:"delete_comment"`
	CreatedAt           *time.Time `json:"created_at"`
	UpdatedAt           *time.Time `json:"updated_at"`
	Year                string     `json:"year,omitempty"`
	PayAmount           float64    `json:"pay_amount"`
	RecAmount           float64    `json:"rec_amount"`
	ExposureType        string     `json:"exposure_type"`
	ExposureCategory    string     `json:"exposure_category"`
}

type NormalizedExposure struct {
	ExposureHeaderID    string  `json:"exposure_header_id"`
	CompanyCode         string  `json:"company_code"`
	Entity              string  `json:"entity"`
	DocumentID          string  `json:"document_id"`
	DocumentDate        string  `json:"document_date"`
	PostingDate         string  `json:"posting_date"`
	ValueDate           string  `json:"value_date"`
	CounterpartyCode    string  `json:"counterparty_code"`
	CounterpartyName    string  `json:"counterparty_name"`
	Currency            string  `json:"currency"`
	TotalOriginalAmount float64 `json:"total_original_amount"`
	TotalOpenAmount     float64 `json:"total_open_amount"`
	AgingDays           int     `json:"aging_days"`
	Status              string  `json:"status"`
	ApprovalStatus      string  `json:"approval_status"`
	ApprovedBy          string  `json:"approved_by"`
	ApprovedAt          string  `json:"approved_at"`
	RejectedBy          string  `json:"rejected_by"`
	RejectedAt          string  `json:"rejected_at"`
	RejectionComment    string  `json:"rejection_comment"`
	DeleteComment       string  `json:"delete_comment"`
	CreatedAt           string  `json:"created_at"`
	UpdatedAt           string  `json:"updated_at"`
	Month               string  `json:"month"`
	Year                int     `json:"year"`
	PayAmount           float64 `json:"pay_amount"`
	RecAmount           float64 `json:"rec_amount"`
	ExposureType        string  `json:"exposure_type"`
	ExposureCategory    string  `json:"exposure_category"`
}

// --------------------- UTILITIES -----------------------

var rates = map[string]float64{
	"USD": 1.0,
	"AUD": 0.68,
	"CAD": 0.75,
	"CHF": 1.1,
	"CNY": 0.14,
	"RMB": 0.14,
	"EUR": 1.09,
	"GBP": 1.28,
	"JPY": 0.0067,
	"SEK": 0.095,
	"INR": 0.0117,
}

func timeToString(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// StreamRowsAsPayload streams rows inside your RespondWithPayload-compatible format
func StreamRowsAsPayload(w http.ResponseWriter, rows pgx.Rows, build func() (NormalizedExposure, error)) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"success":true,"rows":[`))

	enc := json.NewEncoder(w)
	first := true

	for rows.Next() {
		item, err := build()
		if err != nil {
			log.Printf("[STREAM] build error: %v", err)
			continue
		}
		if !first {
			w.Write([]byte(`,`))
		}
		first = false
		_ = enc.Encode(item)
	}

	w.Write([]byte(`]}`))
}

// --------------------- HANDLERS -----------------------

// GetAllExposures - streams all exposures (headers only)
func GetAllExposures(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		query := `
			SELECT
				exposure_header_id,
				company_code,
				entity,
				document_id,
				document_date,
				posting_date,
				value_date,
				exposure_type,
				exposure_category,
				counterparty_code,
				counterparty_name,
				currency,
				total_original_amount,
				total_open_amount,
				status,
				exposure_creation_status AS approval_status,
				approved_by,
				approved_at,
				rejected_by,
				rejected_at,
				rejection_comment,
				delete_comment,
				created_at,
				updated_at
			FROM public.exposure_headers
			WHERE exposure_category IS NOT NULL
			AND lower(coalesce(exposure_creation_status, '')) <> 'approved';
		`

		rows, err := pool.Query(ctx, query)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()

		StreamRowsAsPayload(w, rows, func() (NormalizedExposure, error) {
			e := ExposureResponse{}
			var (
				nhdrID, ncompany, nentity, ndocID, nexposureType, nexposureCategory,
				ncpcode, ncpname, ncurrency, nstatus, napprov, napprovedBy,
				nrejectedBy, nrejectionComment, ndeleteComment *string
				ndocDate, nposting, nvalue, napprovedAt, nrejectedAt, ncreatedAt, nupdatedAt *time.Time
				ntotalOrig, ntotalOpen                                                       *float64
			)

			err := rows.Scan(
				&nhdrID, &ncompany, &nentity, &ndocID, &ndocDate, &nposting, &nvalue,
				&nexposureType, &nexposureCategory, &ncpcode, &ncpname, &ncurrency,
				&ntotalOrig, &ntotalOpen, &nstatus, &napprov, &napprovedBy, &napprovedAt,
				&nrejectedBy, &nrejectedAt, &nrejectionComment, &ndeleteComment, &ncreatedAt, &nupdatedAt,
			)
			if err != nil {
				return NormalizedExposure{}, err
			}

			if nhdrID != nil {
				e.ExposureHeaderID = *nhdrID
			}
			if ncompany != nil {
				e.CompanyCode = *ncompany
			}
			if nentity != nil {
				e.Entity = *nentity
			}
			if ndocID != nil {
				e.DocumentID = *ndocID
			}
			e.DocumentDate, e.PostingDate, e.ValueDate = ndocDate, nposting, nvalue
			if ncpcode != nil {
				e.CounterpartyCode = *ncpcode
			}
			if ncpname != nil {
				e.CounterpartyName = *ncpname
			}
			if ncurrency != nil {
				e.Currency = *ncurrency
			}
			if ntotalOrig != nil {
				e.TotalOriginalAmount = *ntotalOrig
			}
			if ntotalOpen != nil {
				e.TotalOpenAmount = *ntotalOpen
			}
			if nstatus != nil {
				e.Status = *nstatus
			}
			if napprov != nil {
				e.ApprovalStatus = *napprov
			}
			if napprovedBy != nil {
				e.ApprovedBy = *napprovedBy
			}
			e.ApprovedAt = napprovedAt
			if nrejectedBy != nil {
				e.RejectedBy = *nrejectedBy
			}
			e.RejectedAt = nrejectedAt
			if nrejectionComment != nil {
				e.RejectionComment = *nrejectionComment
			}
			if ndeleteComment != nil {
				e.DeleteComment = *ndeleteComment
			}
			e.CreatedAt, e.UpdatedAt = ncreatedAt, nupdatedAt

			if nexposureType != nil {
				et := strings.ToLower(strings.TrimSpace(*nexposureType))
				switch et {
				case "creditor", "creditors":
					et = "creditor"
				case "debitor", "debtor", "debitors":
					et = "debitor"
				case "grn":
					et = "grn"
				}
				e.ExposureType = et
			}
			if nexposureCategory != nil {
				e.ExposureCategory = *nexposureCategory
			}

			cur := strings.ToUpper(strings.TrimSpace(e.Currency))
			rate := rates[cur]
			if rate == 0 {
				rate = 1.0
			}
			amt := e.TotalOpenAmount
			switch strings.ToLower(e.ExposureType) {
			case "debitor", "debtor", "debitors":
				e.RecAmount = math.Abs(amt) * rate
			case "creditor", "grn":
				e.PayAmount = math.Abs(amt) * rate
			}

			// populate Month (e.g. "Jan-25") and Year (e.g. 2025) from ValueDate/PostinDate/DocumentDate
			var monthStr string
			var yearInt int
			var dateForMonth *time.Time
			if e.ValueDate != nil {
				dateForMonth = e.ValueDate
			} else if e.PostingDate != nil {
				dateForMonth = e.PostingDate
			} else if e.DocumentDate != nil {
				dateForMonth = e.DocumentDate
			}
			if dateForMonth != nil {
				if e.PostingDate != nil {
					days := int(e.ValueDate.Sub(*e.PostingDate).Hours() / 24)
					e.AgingDays = &days
				}
				monthStr = dateForMonth.Format("Jan-06")
				yearInt = dateForMonth.Year()
			}

			ne := NormalizedExposure{
				ExposureHeaderID:    e.ExposureHeaderID,
				CompanyCode:         e.CompanyCode,
				Entity:              e.Entity,
				DocumentID:          e.DocumentID,
				DocumentDate:        timeToString(e.DocumentDate),
				PostingDate:         timeToString(e.PostingDate),
				ValueDate:           timeToString(e.ValueDate),
				CounterpartyCode:    e.CounterpartyCode,
				CounterpartyName:    e.CounterpartyName,
				Currency:            e.Currency,
				TotalOriginalAmount: e.TotalOriginalAmount,
				TotalOpenAmount:     e.TotalOpenAmount,
				AgingDays:           0,
				Status:              e.Status,
				ApprovalStatus:      e.ApprovalStatus,
				ApprovedBy:          e.ApprovedBy,
				ApprovedAt:          timeToString(e.ApprovedAt),
				RejectedBy:          e.RejectedBy,
				RejectedAt:          timeToString(e.RejectedAt),
				RejectionComment:    e.RejectionComment,
				DeleteComment:       e.DeleteComment,
				CreatedAt:           timeToString(e.CreatedAt),
				UpdatedAt:           timeToString(e.UpdatedAt),
				Month:               monthStr,
				Year:                yearInt,
				PayAmount:           e.PayAmount,
				RecAmount:           e.RecAmount,
				ExposureType:        e.ExposureType,
				ExposureCategory:    e.ExposureCategory,
			}
			if e.AgingDays != nil {
				ne.AgingDays = *e.AgingDays
			}

			return ne, nil
		})
	}
}

// GetExposuresByYear - streams exposures filtered by year
func GetExposuresByYear(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var payload struct {
			Year string `json:"year"`
		}
		_ = json.NewDecoder(r.Body).Decode(&payload)

		var yParam any
		if payload.Year != "" {
			if y, err := strconv.Atoi(payload.Year); err == nil {
				yParam = y
			}
		}

		query := `
			SELECT
				exposure_header_id,
				company_code,
				entity,
				document_id,
				document_date,
				posting_date,
				value_date,
				exposure_type,
				exposure_category,
				counterparty_code,
				counterparty_name,
				currency,
				total_original_amount,
				total_open_amount,
				status,
				exposure_creation_status AS approval_status,
				approved_by,
				approved_at,
				rejected_by,
				rejected_at,
				rejection_comment,
				delete_comment,
				created_at,
				updated_at
			FROM public.exposure_headers
			WHERE exposure_category IS NOT NULL
			AND lower(coalesce(exposure_creation_status, '')) <> 'approved'
			AND ($1::int IS NULL OR (
				EXTRACT(YEAR FROM value_date) = $1::int OR
				EXTRACT(YEAR FROM posting_date) = $1::int OR
				EXTRACT(YEAR FROM document_date) = $1::int
			))
			ORDER BY value_date DESC;
		`

		rows, err := pool.Query(ctx, query, yParam)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()

		StreamRowsAsPayload(w, rows, func() (NormalizedExposure, error) {
			var e ExposureResponse
			var (
				nhdrID, ncompany, nentity, ndocID, nexposureType, nexposureCategory,
				ncpcode, ncpname, ncurrency, nstatus, napprov, napprovedBy,
				nrejectedBy, nrejectionComment, ndeleteComment *string
				ndocDate, nposting, nvalue, napprovedAt, nrejectedAt, ncreatedAt, nupdatedAt *time.Time
				ntotalOrig, ntotalOpen                                                       *float64
			)

			err := rows.Scan(
				&nhdrID, &ncompany, &nentity, &ndocID, &ndocDate, &nposting, &nvalue,
				&nexposureType, &nexposureCategory, &ncpcode, &ncpname, &ncurrency,
				&ntotalOrig, &ntotalOpen, &nstatus, &napprov, &napprovedBy, &napprovedAt,
				&nrejectedBy, &nrejectedAt, &nrejectionComment, &ndeleteComment, &ncreatedAt, &nupdatedAt,
			)
			if err != nil {
				return NormalizedExposure{}, err
			}

			if nhdrID != nil {
				e.ExposureHeaderID = *nhdrID
			}
			if ncompany != nil {
				e.CompanyCode = *ncompany
			}
			if nentity != nil {
				e.Entity = *nentity
			}
			if ndocID != nil {
				e.DocumentID = *ndocID
			}
			e.DocumentDate, e.PostingDate, e.ValueDate = ndocDate, nposting, nvalue
			if ncpcode != nil {
				e.CounterpartyCode = *ncpcode
			}
			if ncpname != nil {
				e.CounterpartyName = *ncpname
			}
			if ncurrency != nil {
				e.Currency = *ncurrency
			}
			if ntotalOrig != nil {
				e.TotalOriginalAmount = *ntotalOrig
			}
			if ntotalOpen != nil {
				e.TotalOpenAmount = *ntotalOpen
			}
			if nstatus != nil {
				e.Status = *nstatus
			}
			if napprov != nil {
				e.ApprovalStatus = *napprov
			}
			if napprovedBy != nil {
				e.ApprovedBy = *napprovedBy
			}
			e.ApprovedAt = napprovedAt
			if nrejectedBy != nil {
				e.RejectedBy = *nrejectedBy
			}
			e.RejectedAt = nrejectedAt
			if nrejectionComment != nil {
				e.RejectionComment = *nrejectionComment
			}
			if ndeleteComment != nil {
				e.DeleteComment = *ndeleteComment
			}
			e.CreatedAt, e.UpdatedAt = ncreatedAt, nupdatedAt

			if nexposureType != nil {
				et := strings.ToLower(strings.TrimSpace(*nexposureType))
				switch et {
				case "creditor", "creditors":
					et = "creditor"
				case "debitor", "debtor", "debitors":
					et = "debitor"
				case "grn":
					et = "grn"
				}
				e.ExposureType = et
			}
			if nexposureCategory != nil {
				e.ExposureCategory = *nexposureCategory
			}

			if e.Currency == "" {
				e.Currency = "USD"
			}
			rate := rates[strings.ToUpper(strings.TrimSpace(e.Currency))]
			if rate == 0 {
				rate = 1.0
			}
			amt := e.TotalOpenAmount
			switch strings.ToLower(e.ExposureType) {
			case "debitor", "debtor", "debitors":
				e.RecAmount = math.Abs(amt) * rate
			case "creditor", "grn":
				e.PayAmount = math.Abs(amt) * rate
			}

			// populate Month and Year for this record
			var monthStr string
			var yearInt int
			var dateForMonth *time.Time
			if e.ValueDate != nil {
				dateForMonth = e.ValueDate
			} else if e.PostingDate != nil {
				dateForMonth = e.PostingDate
			} else if e.DocumentDate != nil {
				dateForMonth = e.DocumentDate
			}
			if dateForMonth != nil {
				if e.PostingDate != nil && e.ValueDate != nil {
					days := int(e.ValueDate.Sub(*e.PostingDate).Hours() / 24)
					e.AgingDays = &days
				}
				monthStr = dateForMonth.Format("Jan-06")
				yearInt = dateForMonth.Year()
			}

			ne := NormalizedExposure{
				ExposureHeaderID:    e.ExposureHeaderID,
				CompanyCode:         e.CompanyCode,
				Entity:              e.Entity,
				DocumentID:          e.DocumentID,
				DocumentDate:        timeToString(e.DocumentDate),
				PostingDate:         timeToString(e.PostingDate),
				ValueDate:           timeToString(e.ValueDate),
				CounterpartyCode:    e.CounterpartyCode,
				CounterpartyName:    e.CounterpartyName,
				Currency:            e.Currency,
				TotalOriginalAmount: e.TotalOriginalAmount,
				TotalOpenAmount:     e.TotalOpenAmount,
				AgingDays:           0,
				Status:              e.Status,
				ApprovalStatus:      e.ApprovalStatus,
				ApprovedBy:          e.ApprovedBy,
				ApprovedAt:          timeToString(e.ApprovedAt),
				RejectedBy:          e.RejectedBy,
				RejectedAt:          timeToString(e.RejectedAt),
				RejectionComment:    e.RejectionComment,
				DeleteComment:       e.DeleteComment,
				CreatedAt:           timeToString(e.CreatedAt),
				UpdatedAt:           timeToString(e.UpdatedAt),
				Month:               monthStr,
				Year:                yearInt,
				PayAmount:           e.PayAmount,
				RecAmount:           e.RecAmount,
				ExposureType:        e.ExposureType,
				ExposureCategory:    e.ExposureCategory,
			}
			if e.AgingDays != nil {
				ne.AgingDays = *e.AgingDays
			}

			return ne, nil
		})
	}
}
