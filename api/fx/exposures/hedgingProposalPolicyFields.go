package exposures

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type HedgingProposalPolicyRow struct {
	ProposalID       string
	ProposalName     string
	ProcessingStatus string
	Comments         string
	CreatedBy        string
	CreatedAt        string
	UpdatedBy        string
	UpdatedAt        string
	LineCount        int
}

func BuildHedgingProposalPolicyFields(row HedgingProposalPolicyRow) map[string]interface{} {
	return map[string]interface{}{
		"proposal_id":       row.ProposalID,
		"proposal_name":     row.ProposalName,
		"processing_status": row.ProcessingStatus,
		"comments":          row.Comments,
		"created_by":        row.CreatedBy,
		"created_at":        row.CreatedAt,
		"updated_by":        row.UpdatedBy,
		"updated_at":        row.UpdatedAt,
		"line_count":        row.LineCount,
	}
}

func LoadHedgingProposalPolicyRow(ctx context.Context, pool *pgxpool.Pool, proposalID string) (HedgingProposalPolicyRow, error) {
	var row HedgingProposalPolicyRow
	row.ProposalID = proposalID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(d.proposal_name,''), COALESCE(d.processing_status,''), COALESCE(d.comments,''),
		       COALESCE(d.created_by,''), COALESCE(d.created_at::text,''),
		       COALESCE(d.updated_by,''), COALESCE(d.updated_at::text,''),
		       COALESCE(l.line_count,0)
		FROM public.hedging_proposal_document d
		LEFT JOIN LATERAL (
			SELECT COUNT(*) AS line_count
			FROM public.hedging_proposal_document_line pl
			WHERE pl.proposal_id = d.proposal_id
		) l ON true
		WHERE d.proposal_id = $1::uuid`, proposalID,
	).Scan(
		&row.ProposalName, &row.ProcessingStatus, &row.Comments,
		&row.CreatedBy, &row.CreatedAt,
		&row.UpdatedBy, &row.UpdatedAt,
		&row.LineCount,
	)
	if err != nil {
		return row, fmt.Errorf("load hedging proposal row for policy: %w", err)
	}
	return row, nil
}
