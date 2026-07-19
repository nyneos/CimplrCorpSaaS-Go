package forwards

import (
	fxnotification "CimplrCorpSaas/api/fx/notification"
	"CimplrCorpSaas/api/notification/catalog"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	routeForwardManualEntry           = "/fx/forwards/manual-entry"
	routeForwardUploadMulti           = "/fx/forwards/upload-multi"
	routeForwardBulkUpdateStatus      = "/fx/forwards/bulk-update-processing-status"
	routeForwardBulkDelete            = "/fx/forwards/bulk-delete"
	routeForwardUpdateFields          = "/fx/forwards/update-fields"
	routeForwardManualConfirmation    = "/fx/forwards/manual-confirmation-entry"
	routeForwardUploadConfirmations   = "/fx/forwards/upload-confirmations-multi"
	routeForwardCancellationStatus    = "/fx/forwards/cancellation-status-request"
	routeForwardRolloverStatus        = "/fx/forwards/rollover-status-request"
	routeForwardCancelRollAction      = "/fx/forwards/cancel-roll/action"
	routeForwardUploadMTM             = "/fx/forwards/upload-mtm"
	routeForwardMTMUpdateStatus       = "/fx/forwards/mtm/update-processing-status"
)

func triggerForwardBookingNotif(ctx context.Context, pool *pgxpool.Pool, route, action, requestedBy, processingStatus string, bookingIDs []string) {
	if pool == nil || len(bookingIDs) == 0 {
		return
	}
	notifyCtx := context.WithoutCancel(ctx)
	payload := fxnotification.BuildForwardBookingPayload(notifyCtx, pool, bookingIDs, action, requestedBy, processingStatus)
	corr := fmt.Sprintf("FWD-%s/%s/%d", strings.ToUpper(action), bookingIDs[0], time.Now().UnixMilli())
	go catalog.TriggerNotification(notifyCtx, pool, route, corr, payload.ToMap())
}

func triggerForwardConfirmationNotif(ctx context.Context, pool *pgxpool.Pool, route, action, requestedBy, processingStatus string, bookingIDs []string) {
	if pool == nil || len(bookingIDs) == 0 {
		return
	}
	notifyCtx := context.WithoutCancel(ctx)
	payload := fxnotification.BuildForwardConfirmationPayload(notifyCtx, pool, bookingIDs, action, requestedBy, processingStatus)
	corr := fmt.Sprintf("FWDC-%s/%s/%d", strings.ToUpper(action), bookingIDs[0], time.Now().UnixMilli())
	go catalog.TriggerNotification(notifyCtx, pool, route, corr, payload.ToMap())
}

func triggerCancelRollNotif(ctx context.Context, pool *pgxpool.Pool, route, action, requestedBy, status string, items []fxnotification.CancelRollItem) {
	if pool == nil || len(items) == 0 {
		return
	}
	notifyCtx := context.WithoutCancel(ctx)
	payload := fxnotification.BuildCancelRollPayloadFromItems(notifyCtx, pool, items, action, requestedBy, status)
	corrID := items[0].BookingID
	if corrID == "" {
		corrID = "UNKNOWN"
	}
	corr := fmt.Sprintf("FXCR-%s/%s/%d", strings.ToUpper(action), corrID, time.Now().UnixMilli())
	go catalog.TriggerNotification(notifyCtx, pool, route, corr, payload.ToMap())
}

func triggerMTMNotif(ctx context.Context, pool *pgxpool.Pool, route, action, requestedBy, processingStatus string, mtmIDs []string) {
	if pool == nil || len(mtmIDs) == 0 {
		return
	}
	notifyCtx := context.WithoutCancel(ctx)
	payload := fxnotification.BuildMTMPayload(notifyCtx, pool, mtmIDs, action, requestedBy, processingStatus)
	corr := fmt.Sprintf("MTM-%s/%s/%d", strings.ToUpper(action), mtmIDs[0], time.Now().UnixMilli())
	go catalog.TriggerNotification(notifyCtx, pool, route, corr, payload.ToMap())
}

func collectBookingIDsFromRows(rows []map[string]interface{}) []string {
	ids := make([]string, 0, len(rows))
	seen := map[string]bool{}
	for _, row := range rows {
		id := normalizeForwardSystemTransactionID(row["system_transaction_id"])
		if id == "" {
			id = normalizeForwardSystemTransactionID(row["booking_id"])
		}
		if id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids
}

func fetchBookingIDsByUploadKey(ctx context.Context, pool *pgxpool.Pool, uploadKey string) []string {
	if strings.TrimSpace(uploadKey) == "" {
		return nil
	}
	rows, err := pool.Query(ctx, `SELECT system_transaction_id FROM forward_bookings WHERE upload_s3_key = $1`, uploadKey)
	if err != nil {
		return nil
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil && strings.TrimSpace(id) != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

func fetchBookingIDsByConfirmationUploadKey(ctx context.Context, pool *pgxpool.Pool, uploadKey string) []string {
	if strings.TrimSpace(uploadKey) == "" {
		return nil
	}
	rows, err := pool.Query(ctx, `SELECT system_transaction_id FROM forward_bookings WHERE confirmation_upload_s3_key = $1`, uploadKey)
	if err != nil {
		return nil
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil && strings.TrimSpace(id) != "" {
			ids = append(ids, id)
		}
	}
	return ids
}
