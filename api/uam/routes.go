package uam

import (
	middlewares "CimplrCorpSaas/api/middlewares"
	approvalMatrix "CimplrCorpSaas/api/uam/approvalMatrix"
	"CimplrCorpSaas/api/uam/permissions"
	"CimplrCorpSaas/api/uam/role"
	"CimplrCorpSaas/api/uam/user"
	"CimplrCorpSaas/internal/observability"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterUAMRoutes wires every /uam/* route onto mux. Route registration
// only — handler logic lives in the approvalMatrix/permissions/role/user packages.
func RegisterUAMRoutes(mux *http.ServeMux, serviceName string, pgxPool *pgxpool.Pool) {
	mux.HandleFunc("/uam/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("UAM Service is active"))
	})
	mux.Handle("/uam/metrics", observability.MetricsHandler(serviceName))

	midUAM := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(h)
	}

	/*Approval Matrix*/
	mux.Handle("/uam/approval-matrix/create", midUAM(approvalMatrix.CreateApprovalMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/policy-engine/create", midUAM(approvalMatrix.CreatePolicyEngineMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/update", midUAM(approvalMatrix.UpdateApprovalMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/delete", midUAM(approvalMatrix.DeleteApprovalMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/bulk-approve", midUAM(approvalMatrix.BulkApproveMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/bulk-reject", midUAM(approvalMatrix.BulkRejectMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/all", midUAM(approvalMatrix.GetApprovalMatrixAll(pgxPool)))
	mux.Handle("/uam/approval-matrix/detail", midUAM(approvalMatrix.GetApprovalMatrixDetail(pgxPool)))
	mux.Handle("/uam/approval-matrix/audit-history", midUAM(approvalMatrix.GetApprovalMatrixAuditHistory(pgxPool)))
	mux.Handle("/uam/approval-matrix/approved-active", midUAM(approvalMatrix.GetApprovedActiveMatrices(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/add", midUAM(approvalMatrix.AddEyeToMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/update", midUAM(approvalMatrix.UpdateEye(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/delete", midUAM(approvalMatrix.DeleteEye(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/member/add", midUAM(approvalMatrix.AddMemberToEye(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/member/update", midUAM(approvalMatrix.UpdateMember(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/member/delete", midUAM(approvalMatrix.DeleteMember(pgxPool)))
	/*Approval Engine Instances*/
	mux.Handle("/uam/instance/action", midUAM(approvalMatrix.RecordApprovalAction(pgxPool)))
	mux.Handle("/uam/instance/pending", midUAM(approvalMatrix.GetMyPendingApprovals(pgxPool)))
	mux.Handle("/uam/instance/submissions", midUAM(approvalMatrix.GetMySubmissions(pgxPool)))
	mux.Handle("/uam/instance/detail", midUAM(approvalMatrix.GetInstanceDetail(pgxPool)))
	/*users*/
	mux.Handle("/uam/users/create-user", midUAM(http.HandlerFunc(user.CreateUser(pgxPool))))
	mux.Handle("/uam/users/get-users", midUAM(http.HandlerFunc(user.GetUsers(pgxPool))))
	mux.Handle("/uam/users/get-approved-user", midUAM(http.HandlerFunc(user.GetApprovedUser(pgxPool))))
	mux.Handle("/uam/users/get-user-by-id", midUAM(http.HandlerFunc(user.GetUserById(pgxPool))))
	mux.Handle("/uam/users/update-user", midUAM(http.HandlerFunc(user.UpdateUser(pgxPool))))
	mux.Handle("/uam/users/delete-user", midUAM(http.HandlerFunc(user.DeleteUser(pgxPool))))
	mux.Handle("/uam/users/approve-multiple-users", midUAM(http.HandlerFunc(user.ApproveMultipleUsers(pgxPool))))
	mux.Handle("/uam/users/reject-multiple-users", midUAM(http.HandlerFunc(user.RejectMultipleUsers(pgxPool))))
	mux.Handle("/uam/users/audit-history", midUAM(http.HandlerFunc(user.GetUserAuditHistory(pgxPool))))
	/*roles*/
	mux.Handle("/uam/roles/create-role", midUAM(http.HandlerFunc(role.CreateRole(pgxPool))))
	mux.Handle("/uam/roles/page-data", midUAM(http.HandlerFunc(role.GetRolesPageData(pgxPool))))
	mux.Handle("/uam/roles/approve-multiple-roles", midUAM(http.HandlerFunc(role.ApproveMultipleRoles(pgxPool))))
	mux.Handle("/uam/roles/delete-role", midUAM(http.HandlerFunc(role.DeleteRole(pgxPool))))
	mux.Handle("/uam/roles/reject-multiple-roles", midUAM(http.HandlerFunc(role.RejectMultipleRoles(pgxPool))))
	mux.Handle("/uam/roles/update-role", midUAM(http.HandlerFunc(role.UpdateRole(pgxPool))))
	mux.Handle("/uam/roles/get-just-roles", midUAM(http.HandlerFunc(role.GetJustRoles(pgxPool))))
	mux.Handle("/uam/roles/get-roles-for-dropdown", midUAM(http.HandlerFunc(role.GetRolesForDropdown(pgxPool))))
	mux.Handle("/uam/roles/get-user-roles", midUAM(http.HandlerFunc(role.GetJustRolesPERMISSIONapproved(pgxPool))))
	mux.Handle("/uam/roles/get-pending-roles", midUAM(http.HandlerFunc(role.GetPendingRoles(pgxPool))))
	mux.Handle("/uam/roles/audit-history", midUAM(http.HandlerFunc(role.GetRoleAuditHistory(pgxPool))))
	/*Permissions*/
	mux.Handle("/uam/permissions/upsert-role-permissions", midUAM(http.HandlerFunc(permissions.UpsertRolePermissions(pgxPool))))
	mux.Handle("/uam/permissions/permissions-json", midUAM(http.HandlerFunc(permissions.GetRolePermissionsJson(pgxPool))))
	mux.Handle("/uam/permissions/status", midUAM(http.HandlerFunc(permissions.UpdateRolePermissionsStatusByName(pgxPool))))
	mux.Handle("/uam/permissions/approve-reject", midUAM(http.HandlerFunc(permissions.GetRolesStatus(pgxPool))))
	mux.Handle("/uam/permissions/get-role-permissions", midUAM(http.HandlerFunc(permissions.GetRolePermissionsJsonByRoleName(pgxPool))))
	mux.Handle("/uam/permissions/sidebar", midUAM(http.HandlerFunc(permissions.GetSidebarPermissions(pgxPool))))
	mux.Handle("/uam/permissions/requests/all", midUAM(http.HandlerFunc(permissions.GetAllPermissionRequests(pgxPool))))
	mux.Handle("/uam/permissions/requests/role-summary", midUAM(http.HandlerFunc(permissions.GetRolePermissionAuditTable(pgxPool))))
}
