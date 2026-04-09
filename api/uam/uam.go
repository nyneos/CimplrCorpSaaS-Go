package uam

import (
	"CimplrCorpSaas/api"
	// "CimplrCorpSaas/api/auth"
	middlewares "CimplrCorpSaas/api/middlewares"
	approvalMatrix "CimplrCorpSaas/api/uam/approvalMatrix"
	"CimplrCorpSaas/api/uam/permissions" // <-- Import permissions
	"CimplrCorpSaas/api/uam/role"        // <-- Import role
	"CimplrCorpSaas/api/uam/user"        // <-- Import user
	"database/sql"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func StartUAMService(pgxPool *pgxpool.Pool, db *sql.DB, port string) {
	mux := http.NewServeMux()

	mux.HandleFunc("/uam/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("UAM Service is active"))
	})

	/*Approval Matrix*/
	mux.Handle("/uam/approval-matrix/create", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.CreateApprovalMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/update", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.UpdateApprovalMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/delete", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.DeleteApprovalMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.BulkApproveMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.BulkRejectMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/all", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.GetApprovalMatrixAll(pgxPool)))
	mux.Handle("/uam/approval-matrix/detail", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.GetApprovalMatrixDetail(pgxPool)))
	mux.Handle("/uam/approval-matrix/audit-history", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.GetApprovalMatrixAuditHistory(pgxPool)))
	mux.Handle("/uam/approval-matrix/approved-active", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.GetApprovedActiveMatrices(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/add", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.AddEyeToMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/update", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.UpdateEye(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/delete", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.DeleteEye(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/member/add", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.AddMemberToEye(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/member/update", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.UpdateMember(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/member/delete", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.DeleteMember(pgxPool)))
	/*Approval Engine Instances*/
	mux.Handle("/uam/instance/action", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.RecordApprovalAction(pgxPool)))
	mux.Handle("/uam/instance/pending", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.GetMyPendingApprovals(pgxPool)))
	mux.Handle("/uam/instance/submissions", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.GetMySubmissions(pgxPool)))
	mux.Handle("/uam/instance/detail", middlewares.PreValidationMiddleware(pgxPool)(approvalMatrix.GetInstanceDetail(pgxPool)))
	/*users*/
	mux.Handle("/uam/users/create-user", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.CreateUser(db, pgxPool))))
	mux.Handle("/uam/users/get-users", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.GetUsers(db))))
	mux.Handle("/uam/users/get-approved-user", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.GetApprovedUser(db))))
	mux.Handle("/uam/users/get-user-by-id", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.GetUserById(db))))
	mux.Handle("/uam/users/update-user", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.UpdateUser(db))))
	mux.Handle("/uam/users/delete-user", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.DeleteUser(db))))
	mux.Handle("/uam/users/approve-multiple-users", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.ApproveMultipleUsers(db))))
	mux.Handle("/uam/users/reject-multiple-users", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.RejectMultipleUsers(db))))
	/*roles*/
	mux.Handle("/uam/roles/create-role", api.BusinessUnitMiddleware(db)(http.HandlerFunc(role.CreateRole(db))))
	mux.Handle("/uam/roles/page-data", api.BusinessUnitMiddleware(db)(http.HandlerFunc(role.GetRolesPageData(db))))
	mux.Handle("/uam/roles/approve-multiple-roles", api.BusinessUnitMiddleware(db)(http.HandlerFunc(role.ApproveMultipleRoles(db))))
	mux.Handle("/uam/roles/delete-role", api.BusinessUnitMiddleware(db)(http.HandlerFunc(role.DeleteRole(db))))
	mux.Handle("/uam/roles/reject-multiple-roles", api.BusinessUnitMiddleware(db)(http.HandlerFunc(role.RejectMultipleRoles(db))))
	mux.Handle("/uam/roles/update-role", api.BusinessUnitMiddleware(db)(http.HandlerFunc(role.UpdateRole(db))))
	mux.Handle("/uam/roles/get-just-roles", api.BusinessUnitMiddleware(db)(http.HandlerFunc(role.GetJustRoles(db))))
	mux.Handle("/uam/roles/get-user-roles", api.BusinessUnitMiddleware(db)(http.HandlerFunc(role.GetJustRolesPERMISSIONapproved(db))))
	mux.Handle("/uam/roles/get-pending-roles", api.BusinessUnitMiddleware(db)(http.HandlerFunc(role.GetPendingRoles(db))))
	/*Permissions*/
	mux.Handle("/uam/permissions/upsert-role-permissions", api.BusinessUnitMiddleware(db)(http.HandlerFunc(permissions.UpsertRolePermissions(db))))
	mux.Handle("/uam/permissions/permissions-json", api.BusinessUnitMiddleware(db)(http.HandlerFunc(permissions.GetRolePermissionsJson(db))))
	mux.Handle("/uam/permissions/status", api.BusinessUnitMiddleware(db)(http.HandlerFunc(permissions.UpdateRolePermissionsStatusByName(db))))
	mux.Handle("/uam/permissions/approve-reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(permissions.GetRolesStatus(db))))
	mux.Handle("/uam/permissions/get-role-permissions", api.BusinessUnitMiddleware(db)(http.HandlerFunc(permissions.GetRolePermissionsJsonByRoleName(db))))
	mux.Handle("/uam/permissions/sidebar", api.BusinessUnitMiddleware(db)(http.HandlerFunc(permissions.GetSidebarPermissions(db))))

	log.Printf("UAM Service started on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("UAM Service failed: %v", err)
	}
}
