package evidencePack

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// downloadPresignExpiry mirrors the 15-minute expiry every other module in
// this repo uses for additional-file/document presigned URLs (see
// investmentFiles.go, fdBookingWorkbench/confirmation.go, etc.) — reused here
// for consistency rather than inventing a new value.
const downloadPresignExpiry = 15 * time.Minute

// DownloadEvidencePack handles POST /investment/fd-closing/evidence/download.
// Resolves the pack's actual S3 key (its own column if generation already
// completed synchronously enough to have set it, else the DMS join — see
// packWithDmsJoinQuery), generates a presigned URL via the same S3 client/
// helper the rest of the repo's download handlers use
// (s3storage.GetDownloadPresignedURL), and increments download_count.
func DownloadEvidencePack(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			PackID string `json:"pack_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.PackID = strings.TrimSpace(req.PackID)
		if req.PackID == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "pack_id is required")
			return
		}

		ctx := r.Context()

		rows, err := pool.Query(ctx, packWithDmsJoinQuery+`
			WHERE p.pack_id = $1 AND p.is_deleted = false`,
			req.PackID,
		)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingEvidencePack] DownloadEvidencePack query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		out, scanErr := scanRowsToMaps(rows)
		rows.Close()
		if scanErr != nil {
			api.LogErrorForResponse(w, "[FDClosingEvidencePack] DownloadEvidencePack row error: %v", scanErr)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}
		if len(out) == 0 {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Evidence pack not found")
			return
		}
		pack := out[0]

		cycleID, _ := pack["cycle_id"].(string)
		var entityID string
		if err := pool.QueryRow(ctx, `SELECT entity_id FROM `+cycleTable+` WHERE cycle_id = $1`, cycleID).Scan(&entityID); err == nil {
			scope := ctxutil.FromContext(ctx)
			if !scope.HasEntityAccess(entityID) {
				fdclosingcommon.RespondError(w, http.StatusForbidden,
					"Entity ID '"+entityID+"' is not within your authorized access scope.")
				return
			}
		}

		s3Key, _ := pack["s3_key"].(string)
		s3Key = strings.TrimSpace(s3Key)
		if s3Key == "" {
			fdclosingcommon.RespondError(w, http.StatusConflict,
				"Evidence pack file is not ready yet. Generation may still be in progress — try again shortly.")
			return
		}

		downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, s3Key, downloadPresignExpiry)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingEvidencePack] DownloadEvidencePack presign: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to generate download link")
			return
		}

		if _, err := pool.Exec(ctx, `
			UPDATE `+evidencePackTable+`
			SET download_count = download_count + 1
			WHERE pack_id = $1`,
			req.PackID,
		); err != nil {
			// Non-fatal: log but still return the URL — the download itself
			// already succeeded from the caller's point of view.
			api.LogError("[FDClosingEvidencePack] DownloadEvidencePack download_count increment failed: pack_id=%s err=%v", req.PackID, err)
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{
			"pack_id":      req.PackID,
			"download_url": downloadURL,
			"expires_in":   int(downloadPresignExpiry.Seconds()),
		})
		api.LogInfo("[FDClosingEvidencePack] DownloadEvidencePack: pack_id=%s", req.PackID)
	}
}
