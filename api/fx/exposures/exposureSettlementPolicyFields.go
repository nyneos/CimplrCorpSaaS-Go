package exposures

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ExposureSettlementRow struct {
	SettlementID        string
	SettlementMethod    string
	Entity              string
	Currency            string
	SettlementDate      string
	ProcessingStatus    string
	ExposureHeaderID    string
	NewExposureHeaderID string
	Comments            string
	CreatedBy           string
	CreatedAt           string
	UpdatedBy           string
	UpdatedAt           string
	TotalOpenAmount     float64
	TotalSettledAmount  float64
	TotalGainLoss       float64
	LineCount           int
	NewExposureType     string
	NewMaturityDate     string
	NewQuantity         float64
	NewPrice            float64
	NewAmount           float64
	IsDeleted           bool
	MinLineGainLoss     float64
	BankName            string
	LegType             string
}

func BuildExposureSettlementPolicyFields(row ExposureSettlementRow) map[string]interface{} {
	return map[string]interface{}{
		"settlement_id":          row.SettlementID,
		"settlement_method":      row.SettlementMethod,
		"entity":                 row.Entity,
		"currency":               row.Currency,
		"settlement_date":        row.SettlementDate,
		"processing_status":      row.ProcessingStatus,
		"exposure_header_id":     row.ExposureHeaderID,
		"new_exposure_header_id": row.NewExposureHeaderID,
		"comments":               row.Comments,
		"created_by":             row.CreatedBy,
		"created_at":             row.CreatedAt,
		"updated_by":             row.UpdatedBy,
		"updated_at":             row.UpdatedAt,
		"total_open_amount":      row.TotalOpenAmount,
		"total_settled_amount":   row.TotalSettledAmount,
		"total_gain_loss":        row.TotalGainLoss,
		"line_count":             row.LineCount,
		"new_exposure_type":      row.NewExposureType,
		"new_maturity_date":      row.NewMaturityDate,
		"new_quantity":           row.NewQuantity,
		"new_price":              row.NewPrice,
		"new_amount":             row.NewAmount,
		"is_deleted":             row.IsDeleted,
		"min_line_gain_loss":     row.MinLineGainLoss,
		"bank_name":              row.BankName,
		"leg_type":               row.LegType,
	}
}

func LoadExposureSettlementRow(ctx context.Context, pool *pgxpool.Pool, settlementID string) (ExposureSettlementRow, error) {
	var row ExposureSettlementRow
	row.SettlementID = settlementID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(d.settlement_method,''), COALESCE(d.entity,''), COALESCE(d.currency,''),
		       COALESCE(d.settlement_date::text,''), COALESCE(d.processing_status,''),
		       COALESCE(d.new_exposure_header_id,''), COALESCE(d.comments,''),
		       COALESCE(d.created_by,''), COALESCE(d.created_at::text,''),
		       COALESCE(d.updated_by,''), COALESCE(d.updated_at::text,''),
		       COALESCE(d.total_open_amount,0), COALESCE(d.total_settled_amount,0), COALESCE(d.total_gain_loss,0),
		       COALESCE(d.new_exposure_type,''), COALESCE(d.new_maturity_date::text,''),
		       COALESCE(d.new_quantity,0), COALESCE(d.new_price,0), COALESCE(d.new_amount,0),
		       COALESCE(d.is_deleted,false),
		       COALESCE(l.header_ids,''), COALESCE(l.line_count,0), COALESCE(l.min_gain_loss,0),
		       COALESCE(l.bank_names,''), COALESCE(l.leg_types,'')
		FROM public.exposure_settlement_document d
		LEFT JOIN LATERAL (
			SELECT STRING_AGG(DISTINCT esl.exposure_header_id, ', ' ORDER BY esl.exposure_header_id) AS header_ids,
			       COUNT(*)                                                                          AS line_count,
			       MIN(esl.gain_loss)                                                                AS min_gain_loss,
			       STRING_AGG(DISTINCT NULLIF(TRIM(esl.bank_name),''), ', ' ORDER BY NULLIF(TRIM(esl.bank_name),'')) AS bank_names,
			       STRING_AGG(DISTINCT NULLIF(TRIM(esl.leg_type),''),  ', ' ORDER BY NULLIF(TRIM(esl.leg_type),''))  AS leg_types
			FROM public.exposure_settlement_line esl
			WHERE esl.settlement_id = d.settlement_id
		) l ON true
		WHERE d.settlement_id = $1::uuid`, settlementID,
	).Scan(
		&row.SettlementMethod, &row.Entity, &row.Currency,
		&row.SettlementDate, &row.ProcessingStatus,
		&row.NewExposureHeaderID, &row.Comments,
		&row.CreatedBy, &row.CreatedAt,
		&row.UpdatedBy, &row.UpdatedAt,
		&row.TotalOpenAmount, &row.TotalSettledAmount, &row.TotalGainLoss,
		&row.NewExposureType, &row.NewMaturityDate,
		&row.NewQuantity, &row.NewPrice, &row.NewAmount,
		&row.IsDeleted,
		&row.ExposureHeaderID, &row.LineCount, &row.MinLineGainLoss,
		&row.BankName, &row.LegType,
	)
	if err != nil {
		return row, fmt.Errorf("load exposure settlement row for policy: %w", err)
	}
	return row, nil
}

func ApplyExposureSettlementLines(row ExposureSettlementRow, lines []settlementLineInput) ExposureSettlementRow {
	row.LineCount = len(lines)
	row.MinLineGainLoss = 0
	headerIDs := make([]string, 0, len(lines))
	banks := make([]string, 0, len(lines))
	legs := make([]string, 0, len(lines))
	haveGainLoss := false
	for _, l := range lines {
		if l.GainLoss != nil {
			if !haveGainLoss || *l.GainLoss < row.MinLineGainLoss {
				row.MinLineGainLoss = *l.GainLoss
			}
			haveGainLoss = true
		}
		headerIDs = appendDistinctSettlementValue(headerIDs, l.ExposureHeaderID)
		banks = appendDistinctSettlementValue(banks, l.BankName)
		legs = appendDistinctSettlementValue(legs, l.LegType)
	}
	row.ExposureHeaderID = joinSettlementValues(headerIDs)
	row.BankName = joinSettlementValues(banks)
	row.LegType = joinSettlementValues(legs)
	return row
}

func appendDistinctSettlementValue(out []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return out
	}
	for _, existing := range out {
		if existing == value {
			return out
		}
	}
	return append(out, value)
}

func joinSettlementValues(values []string) string {
	if len(values) == 0 {
		return ""
	}
	sorted := append([]string{}, values...)
	sort.Strings(sorted)
	return strings.Join(sorted, ", ")
}

func settlementAmountValue(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func settlementTotalGainLoss(lines []settlementLineInput) float64 {
	total := 0.0
	for _, l := range lines {
		if l.GainLoss != nil {
			total += *l.GainLoss
		}
	}
	return math.Round(total*100) / 100
}
