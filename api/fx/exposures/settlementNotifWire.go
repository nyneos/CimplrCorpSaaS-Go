package exposures

import (
	"context"
	"strings"

	fxnotif "CimplrCorpSaas/api/fx/notification"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	routeSettlementSave    = "/fx/exposures/settlements/save"
	routeSettlementEdit    = "/fx/exposures/settlements/edit"
	routeSettlementApprove = "/fx/exposures/settlements/approve"
	routeSettlementReject  = "/fx/exposures/settlements/reject"
	routeSettlementDelete  = "/fx/exposures/settlements/delete"
)

type settlementNotifInput struct {
	Route            string
	Action           string
	UserID           string
	RequestedBy      string
	ProcessingStatus string
	CheckerComment   string
	SettlementIDs    []string
}

func triggerSettlementNotif(ctx context.Context, pool *pgxpool.Pool, in settlementNotifInput) {
	if pool == nil || strings.TrimSpace(in.Route) == "" || len(in.SettlementIDs) == 0 {
		return
	}
	notifyCtx := context.WithoutCancel(ctx)
	payload := fxnotif.BuildSettlementPayload(notifyCtx, pool, fxnotif.SettlementPayloadInput{
		SettlementIDs:    in.SettlementIDs,
		Action:           in.Action,
		RequestedBy:      in.RequestedBy,
		ProcessingStatus: in.ProcessingStatus,
		CheckerComment:   in.CheckerComment,
	})
	payloadMap := payload.ToMap()
	if in.UserID != "" {
		payloadMap["UserID"] = in.UserID
	}
	prefix := "FXSETTLE-" + strings.ToUpper(strings.TrimSpace(in.Action))
	fxnotif.TriggerFX(notifyCtx, pool, in.Route, fxnotif.CorrelationID(prefix, in.SettlementIDs[0]), payloadMap)
}
