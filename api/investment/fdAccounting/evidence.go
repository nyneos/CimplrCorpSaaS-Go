package fdAccounting

import (
	"encoding/json"
	"net/http"
	"path"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func DownloadJournalEvidence(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntryID string `json:"entry_id"`
			Preview bool   `json:"preview"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.EntryID) == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "entry_id is required")
			return
		}
		entryID := strings.TrimSpace(req.EntryID)
		ctx := r.Context()

		args := []interface{}{}
		q := `SELECT COALESCE(je.evidence_s3_key,'') FROM ` + journalTable + ` je
			WHERE COALESCE(je.is_deleted,false) = false` + scopeClause(ctxutil.FromContext(ctx), &args)
		args = append(args, entryID)
		q += " AND je.entry_id = $" + itoa(len(args))

		var key string
		err := pool.QueryRow(ctx, q, args...).Scan(&key)
		if err == pgx.ErrNoRows {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "journal entry not found")
			return
		}
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		if key == "" {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "no evidence file uploaded for this entry")
			return
		}

		var url string
		if req.Preview {
			url, err = s3storage.GetInlinePresignedURL(ctx, key, 15*time.Minute)
		} else {
			url, err = s3storage.GetDownloadPresignedURL(ctx, key, 15*time.Minute)
		}
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "presign evidence file: "+err.Error())
			return
		}
		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{
			"download_url": url,
			"file_name":    path.Base(key),
		})
	}
}
