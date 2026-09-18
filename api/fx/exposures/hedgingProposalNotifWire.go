package exposures

import (
	"context"
	"strings"

	fxnotif "CimplrCorpSaas/api/fx/notification"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	routeHedgingProposalSave    = "/fx/exposures/hedging-proposals/save"
	routeHedgingProposalApprove = "/fx/exposures/hedging-proposals/approve"
	routeHedgingProposalReject  = "/fx/exposures/hedging-proposals/reject"
	routeHedgingProposalDelete  = "/fx/exposures/hedging-proposals/delete"
)

type hedgingProposalNotifInput struct {
	Route            string
	Action           string
	UserID           string
	RequestedBy      string
	ProcessingStatus string
	CheckerComment   string
	ProposalIDs      []string
}

func triggerHedgingProposalNotif(ctx context.Context, pool *pgxpool.Pool, in hedgingProposalNotifInput) {
	if pool == nil || strings.TrimSpace(in.Route) == "" || len(in.ProposalIDs) == 0 {
		return
	}
	notifyCtx := context.WithoutCancel(ctx)
	payload := fxnotif.BuildHedgingProposalPayload(notifyCtx, pool, fxnotif.HedgingProposalPayloadInput{
		ProposalIDs:      in.ProposalIDs,
		Action:           in.Action,
		RequestedBy:      in.RequestedBy,
		ProcessingStatus: in.ProcessingStatus,
		CheckerComment:   in.CheckerComment,
	})
	payloadMap := payload.ToMap()
	if in.UserID != "" {
		payloadMap["UserID"] = in.UserID
	}
	prefix := "FXHEDGEPROP-" + strings.ToUpper(strings.TrimSpace(in.Action))
	fxnotif.TriggerFX(notifyCtx, pool, in.Route, fxnotif.CorrelationID(prefix, in.ProposalIDs[0]), payloadMap)
}
