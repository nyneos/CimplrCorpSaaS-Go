package notification

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NotifyExposureUpload builds an upload payload and dispatches after a successful commit.
func NotifyExposureUpload(ctx context.Context, pool *pgxpool.Pool, sourceRoute, batchID, userID, uploadedBy string) {
	if pool == nil || strings.TrimSpace(batchID) == "" {
		return
	}
	notifyCtx := context.WithoutCancel(ctx)
	actor := uploadedBy
	if actor == "" {
		actor = userID
	}
	payload := BuildExposureUploadPayload(notifyCtx, pool, batchID, actor)
	payloadMap := payload.ToMap()
	if userID != "" {
		payloadMap["UserID"] = userID
	}
	TriggerFX(notifyCtx, pool, sourceRoute, CorrelationID("FXUPLOAD", batchID), payloadMap)
}

// NotifyExposureBulkAction builds a bulk-action payload and dispatches after success.
func NotifyExposureBulkAction(
	ctx context.Context,
	pool *pgxpool.Pool,
	sourceRoute string,
	action string,
	userID string,
	requestedBy string,
	checkerComment string,
	exposureIDs []string,
	resultBuckets map[string][]string,
) {
	if pool == nil || len(exposureIDs) == 0 {
		return
	}
	notifyCtx := context.WithoutCancel(ctx)
	actor := requestedBy
	if actor == "" {
		actor = userID
	}
	approved := resultBuckets["approved"]
	rejected := resultBuckets["rejected"]
	deleted := resultBuckets["deleted"]
	if rejected == nil {
		rejected = resultBuckets["updated"]
	}

	payload := BuildExposureBulkActionPayload(
		notifyCtx, pool, exposureIDs, action, actor, checkerComment,
		approved, rejected, deleted,
	)
	payloadMap := payload.ToMap()
	if userID != "" {
		payloadMap["UserID"] = userID
	}

	prefix := "FXACTION"
	switch action {
	case ActionApprove:
		prefix = "FXAPPROVE"
	case ActionReject:
		prefix = "FXREJECT"
	case ActionDelete:
		prefix = "FXDELETE"
	case ActionUpdate, ActionEdit:
		prefix = "FXUPDATE"
	case ActionLink:
		prefix = "FXLINK"
	case ActionUpload:
		prefix = "FXUPLOAD"
	}
	corrKey := userID
	if len(exposureIDs) > 0 {
		corrKey = exposureIDs[0]
	}
	TriggerFX(notifyCtx, pool, sourceRoute, CorrelationID(prefix, corrKey), payloadMap)
}
