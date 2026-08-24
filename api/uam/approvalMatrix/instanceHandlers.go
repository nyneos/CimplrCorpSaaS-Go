package approvalMatrix

import (
	"CimplrCorpSaas/api"
	approvalengine "CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── RecordApprovalAction ─────────────────────────────────────────────────────
// POST /uam/instance/action
//
// Body:
//
//	{
//	  "user_id":         "USR-...",
//	  "instance_eye_id": "IEYE-...",
//	  "action_type":     constants.StatusApproved | constants.StatusRejected,
//	  "actor_role_id":   "optional-role-id",
//	  "comment":         "optional comment"
//	}
func RecordApprovalAction(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			UserID        string `json:"user_id"`
			InstanceEyeID string `json:"instance_eye_id"`
			ActionType    string `json:"action_type"`
			ActorRoleID   string `json:"actor_role_id"`
			Comment       string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body: "+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}
		if req.InstanceEyeID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "instance_eye_id is required")
			return
		}
		if req.ActionType != constants.StatusApproved && req.ActionType != constants.StatusRejected {
			api.RespondWithError(w, http.StatusBadRequest, "action_type must be APPROVED or REJECTED")
			return
		}

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "No active session found for this user")
			return
		}

		ctx := r.Context()
		err := approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
			InstanceEyeID: req.InstanceEyeID,
			ActorUserID:   req.UserID,
			ActorEmail:    userEmail,
			ActorRoleID:   req.ActorRoleID,
			ActionType:    req.ActionType,
			Comment:       req.Comment,
		})
		if err != nil {
			msg := err.Error()
			lower := strings.ToLower(msg)
			switch {
			case strings.Contains(lower, "not found"):
				api.RespondWithError(w, http.StatusNotFound, msg)
			case strings.Contains(lower, "unauthorized"):
				api.RespondWithError(w, http.StatusForbidden, msg)
			case strings.Contains(lower, "not active"), strings.Contains(lower, "not pending"):
				api.RespondWithError(w, http.StatusBadRequest, msg)
			case strings.Contains(lower, "already acted"):
				api.RespondWithError(w, http.StatusConflict, msg)
			default:
				api.RespondWithError(w, http.StatusInternalServerError, "Approval action failed: "+msg)
			}
			return
		}

		api.LogInfo("[ApprovalEngine] ApprovalAction: eye=%s action=%s by=%s",
			req.InstanceEyeID, req.ActionType, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"instance_eye_id": req.InstanceEyeID,
			"action_type":     req.ActionType,
			"acted_by":        userEmail,
		})
	}
}

// ─── GetMyPendingApprovals ────────────────────────────────────────────────────
// GET /uam/instance/pending?user_id=USR-...
//
// Returns all ACTIVE approval instances where the calling user is an eligible
// approver (direct membership, role membership, or escalation target).
// Ordered by SLA deadline ASC (most urgent first).
func GetMyPendingApprovals(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := r.URL.Query().Get("user_id")
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}

		ctx := r.Context()
		summaries, err := approvalengine.GetPendingForUser(ctx, pgxPool, userID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch pending approvals: "+err.Error())
			return
		}

		api.LogInfo("[ApprovalEngine] GetPendingApprovals: user=%s count=%d", userID, len(summaries))
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rows":  summaries,
			"count": len(summaries),
		})
	}
}

// ─── GetMySubmissions ─────────────────────────────────────────────────────────
// GET /uam/instance/submissions?user_id=USR-...
//
// Returns all approval instances submitted BY the calling user, newest first.
// Shows where each submission currently stands in the approval chain.
func GetMySubmissions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		userID := r.URL.Query().Get("user_id")
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}

		ctx := r.Context()
		summaries, err := approvalengine.GetMySubmissions(ctx, pgxPool, userID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch submissions: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rows":  summaries,
			"count": len(summaries),
		})
	}
}

// ─── GetInstanceDetail ────────────────────────────────────────────────────────
// GET /uam/instance/detail?instance_id=INST-...&user_id=USR-...
// GET /uam/instance/detail?record_id=...&module_code=CASH|FIXED_DEPOSIT|INVESTMENT_MF|FX
//
// instance_id is preferred. When it is omitted, the newest matching instance is
// resolved from record_id (optionally scoped by module_code) so cash / FD / MF /
// FX audit pages can load the matrix without a pre-joined instance id.
//
// Returns the full approval workflow enriched with per-eye approver identities:
//
//	"eyes": [
//	  {
//	    "position": 1,
//	    "status": "ACTIVE",
//	    "approvers": {
//	      "approver_1": {
//	        "user_email": "cfo@acme.com",
//	        "user_name":  "Alice",
//	        "action_taken": constants.StatusApproved,
//	        "action_at":    "2026-03-18T23:02:44Z",
//	        "action_comment": "looks good"
//	      },
//	      "approver_2": {
//	        "role_name": "Treasury Manager",
//	        "action_taken": ""   // not yet acted
//	      }
//	    },
//	    "action_log": [...]
//	  }
//	],
//	"viewer_can_act":       true,
//	"viewer_active_eye_id": "IEYE-..."
//
// user_id is optional; without it viewer_can_act is always false.
func GetInstanceDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		instanceID := strings.TrimSpace(r.URL.Query().Get("instance_id"))
		recordID := strings.TrimSpace(r.URL.Query().Get("record_id"))
		moduleCode := strings.TrimSpace(r.URL.Query().Get("module_code"))
		viewerUserID := r.URL.Query().Get("user_id") // optional

		ctx := r.Context()
		if instanceID == "" {
			if recordID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "instance_id or record_id is required")
				return
			}
			resolved, lookupErr := approvalengine.LookupLatestInstanceID(ctx, pgxPool, moduleCode, recordID)
			if lookupErr != nil {
				if strings.Contains(lookupErr.Error(), "not found") {
					api.RespondWithError(w, http.StatusNotFound, lookupErr.Error())
					return
				}
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to resolve instance: "+lookupErr.Error())
				return
			}
			instanceID = resolved
		}

		detail, err := approvalengine.GetRichInstanceDetail(ctx, pgxPool, instanceID, viewerUserID)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				api.RespondWithError(w, http.StatusNotFound, err.Error())
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch instance: "+err.Error())
			return
		}

		api.RespondEnvelopeSuccess(w, "Instance detail fetched", detail)
	}
}
