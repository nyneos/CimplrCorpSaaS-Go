package projection

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func UploadCashflowProposalSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("[UploadCashflowProposalSimple] Start %s %s", r.Method, r.URL.Path)
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[UploadCashflowProposalSimple] Panic recovered: %v", rec)
				api.Error(w, http.StatusInternalServerError, constants.ErrInternalServer)
			}
			log.Printf("[UploadCashflowProposalSimple] Finished in %s", time.Since(start))
		}()

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.Error(w, http.StatusBadRequest, constants.ErrFailedToParseForm+err.Error())
			return
		}

		userID := r.FormValue(constants.KeyUserID)
		proposalName := strings.TrimSpace(r.FormValue("proposal_name"))
		recurrenceType := strings.TrimSpace(r.FormValue("proposal_type"))
		effectiveDate := strings.TrimSpace(r.FormValue("effective_date"))
		currency := strings.TrimSpace(r.FormValue("currency"))

		if userID == "" || proposalName == "" || currency == "" {
			api.Error(w, http.StatusBadRequest, "user_id, proposal_name and currency are required")
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
			api.Error(w, http.StatusBadRequest, "No file uploaded")
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
			api.Error(w, statusCode, err.Error())
			return
		}

		api.Success(w, http.StatusOK, map[string]interface{}{
			"proposal_id":   proposalID,
			"imported_rows": importedRows,
		}, "Proposal, items, projections & audit committed successfully")
	}
}

