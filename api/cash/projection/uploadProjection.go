package projection

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger")

func UploadCashflowProposalSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		logger.LogInfo("[UploadCashflowProposalSimple] Start %s %s", r.Method, r.URL.Path)
		defer func() {
			if rec := recover(); rec != nil {
				logger.LogError("[UploadCashflowProposalSimple] Panic recovered: %v", rec)
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrInternalServer)
			}
			logger.LogInfo("[UploadCashflowProposalSimple] Finished in %s", time.Since(start))
		}()

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseForm+err.Error())
			return
		}

		userID := r.FormValue(constants.KeyUserID)
		proposalName := strings.TrimSpace(r.FormValue("proposal_name"))
		recurrenceType := strings.TrimSpace(r.FormValue("proposal_type"))
		effectiveDate := strings.TrimSpace(r.FormValue("effective_date"))
		currency := strings.TrimSpace(r.FormValue("currency"))

		if userID == "" || proposalName == "" || currency == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id, proposal_name and currency are required")
			return
		}
		if effectiveDate == "" {
			effectiveDate = time.Now().Format(constants.DateFormat)
		}

		userEmail := ""
		// userEmail := "admin@example.com"
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userEmail = s.Email
				break
			}
		}
		requestedBy := strings.TrimSpace(userEmail)
		if requestedBy == "" {
			requestedBy = userID
		}

		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No file uploaded")
			return
		}

		proposalID, importedRows, statusCode, err := uploadCashflowProposalService(
			r.Context(),
			pgxPool,
			files[0],
			requestedBy,
			proposalUploadOpts{
				ProposalName:   proposalName,
				RecurrenceType: recurrenceType,
				EffectiveDate:  effectiveDate,
				Currency:       currency,
			},
		)
		if err != nil {
			api.RespondWithError(w, statusCode, err.Error())
			return
		}

		resp := map[string]interface{}{
			constants.ValueSuccess: true,
			"proposal_id":          proposalID,
			"imported_rows":        importedRows,
			"message":              "Proposal, items, projections & audit committed successfully",
		}
		json.NewEncoder(w).Encode(resp)
	}
}
