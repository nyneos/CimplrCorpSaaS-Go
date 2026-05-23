package uam

import (
	middlewares "CimplrCorpSaas/api/middlewares"
	approvalMatrix "CimplrCorpSaas/api/uam/approvalMatrix"
	"CimplrCorpSaas/api/uam/permissions"
	"CimplrCorpSaas/api/uam/role"
	"CimplrCorpSaas/api/uam/user"
	"CimplrCorpSaas/internal/dbutil"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func StartUAMService(db *sql.DB, port string) {
	mux := http.NewServeMux()

	// Build pgx pool for approval matrix handlers (PreValidationMiddleware pattern)
	pgxPool := func() *pgxpool.Pool {
		user := os.Getenv("DB_USER")
		pass := os.Getenv("DB_PASSWORD")
		host := os.Getenv("DB_HOST")
		port := os.Getenv("DB_PORT")
		name := os.Getenv("DB_NAME")
		sslMode := dbutil.EffectiveSSLMode(host)
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, port, name, sslMode)
		pool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			logger.LogError("UAM: failed to connect to pgxpool DB: %v", err)
		}
		return pool
	}()
	defer pgxPool.Close()
	mux.HandleFunc("/uam/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("UAM Service is active"))
	})

	prevalidate := middlewares.PreValidationMiddleware(pgxPool)

	/*Approval Matrix*/
	mux.Handle("/uam/approval-matrix/create", prevalidate(approvalMatrix.CreateApprovalMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/update", prevalidate(approvalMatrix.UpdateApprovalMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/delete", prevalidate(approvalMatrix.DeleteApprovalMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/bulk-approve", prevalidate(approvalMatrix.BulkApproveMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/bulk-reject", prevalidate(approvalMatrix.BulkRejectMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/all", prevalidate(approvalMatrix.GetApprovalMatrixAll(pgxPool)))
	mux.Handle("/uam/approval-matrix/detail", prevalidate(approvalMatrix.GetApprovalMatrixDetail(pgxPool)))
	mux.Handle("/uam/approval-matrix/audit-history", prevalidate(approvalMatrix.GetApprovalMatrixAuditHistory(pgxPool)))
	mux.Handle("/uam/approval-matrix/approved-active", prevalidate(approvalMatrix.GetApprovedActiveMatrices(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/add", prevalidate(approvalMatrix.AddEyeToMatrix(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/update", prevalidate(approvalMatrix.UpdateEye(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/delete", prevalidate(approvalMatrix.DeleteEye(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/member/add", prevalidate(approvalMatrix.AddMemberToEye(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/member/update", prevalidate(approvalMatrix.UpdateMember(pgxPool)))
	mux.Handle("/uam/approval-matrix/eye/member/delete", prevalidate(approvalMatrix.DeleteMember(pgxPool)))
	/*Approval Engine Instances*/
	mux.Handle("/uam/instance/action", prevalidate(approvalMatrix.RecordApprovalAction(pgxPool)))
	mux.Handle("/uam/instance/pending", prevalidate(approvalMatrix.GetMyPendingApprovals(pgxPool)))
	mux.Handle("/uam/instance/submissions", prevalidate(approvalMatrix.GetMySubmissions(pgxPool)))
	mux.Handle("/uam/instance/detail", prevalidate(approvalMatrix.GetInstanceDetail(pgxPool)))
	/*users*/
	mux.Handle("/uam/users/create-user", prevalidate(http.HandlerFunc(user.CreateUser(db, pgxPool))))
	mux.Handle("/uam/users/get-users", prevalidate(http.HandlerFunc(user.GetUsers(db))))
	mux.Handle("/uam/users/get-approved-user", prevalidate(http.HandlerFunc(user.GetApprovedUser(db))))
	mux.Handle("/uam/users/get-user-by-id", prevalidate(http.HandlerFunc(user.GetUserById(db))))
	mux.Handle("/uam/users/update-user", prevalidate(http.HandlerFunc(user.UpdateUser(db))))
	mux.Handle("/uam/users/delete-user", prevalidate(http.HandlerFunc(user.DeleteUser(db))))
	mux.Handle("/uam/users/approve-multiple-users", prevalidate(http.HandlerFunc(user.ApproveMultipleUsers(db))))
	mux.Handle("/uam/users/reject-multiple-users", prevalidate(http.HandlerFunc(user.RejectMultipleUsers(db))))
	mux.Handle("/uam/users/audit-history", prevalidate(http.HandlerFunc(user.GetUserAuditHistory(db))))
	/*roles*/
	mux.Handle("/uam/roles/create-role", prevalidate(http.HandlerFunc(role.CreateRole(db))))
	mux.Handle("/uam/roles/page-data", prevalidate(http.HandlerFunc(role.GetRolesPageData(db))))
	mux.Handle("/uam/roles/approve-multiple-roles", prevalidate(http.HandlerFunc(role.ApproveMultipleRoles(db))))
	mux.Handle("/uam/roles/delete-role", prevalidate(http.HandlerFunc(role.DeleteRole(db))))
	mux.Handle("/uam/roles/reject-multiple-roles", prevalidate(http.HandlerFunc(role.RejectMultipleRoles(db))))
	mux.Handle("/uam/roles/update-role", prevalidate(http.HandlerFunc(role.UpdateRole(db))))
	mux.Handle("/uam/roles/get-just-roles", prevalidate(http.HandlerFunc(role.GetJustRoles(db))))
	mux.Handle("/uam/roles/get-user-roles", prevalidate(http.HandlerFunc(role.GetJustRolesPERMISSIONapproved(db))))
	mux.Handle("/uam/roles/get-pending-roles", prevalidate(http.HandlerFunc(role.GetPendingRoles(db))))
	mux.Handle("/uam/roles/audit-history", prevalidate(http.HandlerFunc(role.GetRoleAuditHistory(db))))
	/*Permissions*/
	mux.Handle("/uam/permissions/upsert-role-permissions", prevalidate(http.HandlerFunc(permissions.UpsertRolePermissions(db))))
	mux.Handle("/uam/permissions/permissions-json", prevalidate(http.HandlerFunc(permissions.GetRolePermissionsJson(db))))
	mux.Handle("/uam/permissions/status", prevalidate(http.HandlerFunc(permissions.UpdateRolePermissionsStatusByName(db))))
	mux.Handle("/uam/permissions/approve-reject", prevalidate(http.HandlerFunc(permissions.GetRolesStatus(db))))
	mux.Handle("/uam/permissions/get-role-permissions", prevalidate(http.HandlerFunc(permissions.GetRolePermissionsJsonByRoleName(db))))
	mux.Handle("/uam/permissions/sidebar", prevalidate(http.HandlerFunc(permissions.GetSidebarPermissions(db))))
	mux.Handle("/uam/permissions/requests/all", prevalidate(http.HandlerFunc(permissions.GetAllPermissionRequests(db))))
	mux.Handle("/uam/permissions/requests/role-summary", prevalidate(http.HandlerFunc(permissions.GetRolePermissionAuditTable(db))))

	logger.LogInfo("UAM Service started on :%s", port)
	err := http.ListenAndServe(":"+port, mux)
	if err != nil {
		logger.LogError("UAM Service failed: %v", err)
	}
}
