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

// BulkActionNotifyInput groups the fields needed to dispatch a bulk-action
// notification, keeping the function signature under the project's
// parameter-count limit.
type BulkActionNotifyInput struct {
	SourceRoute    string
	Action         string
	UserID         string
	RequestedBy    string
	CheckerComment string
	ExposureIDs    []string
	ResultBuckets  map[string][]string
}

// NotifyExposureBulkAction builds a bulk-action payload and dispatches after success.
func NotifyExposureBulkAction(ctx context.Context, pool *pgxpool.Pool, in BulkActionNotifyInput) {
	if pool == nil || len(in.ExposureIDs) == 0 {
		return
	}
	notifyCtx := context.WithoutCancel(ctx)
	actor := in.RequestedBy
	if actor == "" {
		actor = in.UserID
	}
	approved := in.ResultBuckets["approved"]
	rejected := in.ResultBuckets["rejected"]
	deleted := in.ResultBuckets["deleted"]
	if rejected == nil {
		rejected = in.ResultBuckets["updated"]
	}

	payload := BuildExposureBulkActionPayload(notifyCtx, pool, ExposureBulkActionInput{
		ExposureIDs: in.ExposureIDs, Action: in.Action, RequestedBy: actor, CheckerComment: in.CheckerComment,
		ApprovedIDs: approved, RejectedIDs: rejected, DeletedIDs: deleted,
	})
	payloadMap := payload.ToMap()
	if in.UserID != "" {
		payloadMap["UserID"] = in.UserID
	}

	prefix := "FXACTION"
	switch in.Action {
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
	corrKey := in.UserID
	if len(in.ExposureIDs) > 0 {
		corrKey = in.ExposureIDs[0]
	}
	TriggerFX(notifyCtx, pool, in.SourceRoute, CorrelationID(prefix, corrKey), payloadMap)
}
