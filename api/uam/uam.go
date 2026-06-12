package uam

import (
	middlewares "CimplrCorpSaas/api/middlewares"
	approvalMatrix "CimplrCorpSaas/api/uam/approvalMatrix"
	"CimplrCorpSaas/api/uam/permissions" // <-- Import permissions
	"CimplrCorpSaas/api/uam/role"        // <-- Import role
	"CimplrCorpSaas/api/uam/user"        // <-- Import user
	"CimplrCorpSaas/internal/dbutil"
	"CimplrCorpSaas/internal/observability"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func NewUAMServer(db *sql.DB, port string) (*http.Server, *pgxpool.Pool, error) {
	const serviceName = "uam"
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
			return nil
		}
		pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := pool.Ping(pingCtx); err != nil {
			pool.Close()
			return nil
		}
		return pool
	}()
	if pgxPool == nil {
		return nil, nil, fmt.Errorf("UAM: failed to initialize pgxpool DB")
	}
	mux.HandleFunc("/uam/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("UAM Service is active"))
	})
	mux.Handle("/uam/metrics", observability.MetricsHandler(serviceName))

	midUAM := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(h)
	}

	/*Approval Matrix*/
	mux.Handle("/uam/approval-matrix/create", midUAM(approvalMatrix.CreateApprovalMatrix(pgxPool)))
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
	mux.Handle("/uam/users/create-user", midUAM(http.HandlerFunc(user.CreateUser(db, pgxPool))))
	mux.Handle("/uam/users/get-users", midUAM(http.HandlerFunc(user.GetUsers(db))))
	mux.Handle("/uam/users/get-approved-user", midUAM(http.HandlerFunc(user.GetApprovedUser(db))))
	mux.Handle("/uam/users/get-user-by-id", midUAM(http.HandlerFunc(user.GetUserById(db))))
	mux.Handle("/uam/users/update-user", midUAM(http.HandlerFunc(user.UpdateUser(db))))
	mux.Handle("/uam/users/delete-user", midUAM(http.HandlerFunc(user.DeleteUser(db))))
	mux.Handle("/uam/users/approve-multiple-users", midUAM(http.HandlerFunc(user.ApproveMultipleUsers(db))))
	mux.Handle("/uam/users/reject-multiple-users", midUAM(http.HandlerFunc(user.RejectMultipleUsers(db))))
	mux.Handle("/uam/users/audit-history", midUAM(http.HandlerFunc(user.GetUserAuditHistory(db))))
	/*roles*/
	mux.Handle("/uam/roles/create-role", midUAM(http.HandlerFunc(role.CreateRole(db))))
	mux.Handle("/uam/roles/page-data", midUAM(http.HandlerFunc(role.GetRolesPageData(db))))
	mux.Handle("/uam/roles/approve-multiple-roles", midUAM(http.HandlerFunc(role.ApproveMultipleRoles(db))))
	mux.Handle("/uam/roles/delete-role", midUAM(http.HandlerFunc(role.DeleteRole(db))))
	mux.Handle("/uam/roles/reject-multiple-roles", midUAM(http.HandlerFunc(role.RejectMultipleRoles(db))))
	mux.Handle("/uam/roles/update-role", midUAM(http.HandlerFunc(role.UpdateRole(db))))
	mux.Handle("/uam/roles/get-just-roles", midUAM(http.HandlerFunc(role.GetJustRoles(db))))
	mux.Handle("/uam/roles/get-roles-for-dropdown", midUAM(http.HandlerFunc(role.GetRolesForDropdown(db))))
	mux.Handle("/uam/roles/get-user-roles", midUAM(http.HandlerFunc(role.GetJustRolesPERMISSIONapproved(db))))
	mux.Handle("/uam/roles/get-pending-roles", midUAM(http.HandlerFunc(role.GetPendingRoles(db))))
	mux.Handle("/uam/roles/audit-history", midUAM(http.HandlerFunc(role.GetRoleAuditHistory(db))))
	/*Permissions*/
	mux.Handle("/uam/permissions/upsert-role-permissions", midUAM(http.HandlerFunc(permissions.UpsertRolePermissions(db))))
	mux.Handle("/uam/permissions/permissions-json", midUAM(http.HandlerFunc(permissions.GetRolePermissionsJson(db))))
	mux.Handle("/uam/permissions/status", midUAM(http.HandlerFunc(permissions.UpdateRolePermissionsStatusByName(db))))
	mux.Handle("/uam/permissions/approve-reject", midUAM(http.HandlerFunc(permissions.GetRolesStatus(db))))
	mux.Handle("/uam/permissions/get-role-permissions", midUAM(http.HandlerFunc(permissions.GetRolePermissionsJsonByRoleName(db))))
	mux.Handle("/uam/permissions/sidebar", midUAM(http.HandlerFunc(permissions.GetSidebarPermissions(db))))
	mux.Handle("/uam/permissions/requests/all", midUAM(http.HandlerFunc(permissions.GetAllPermissionRequests(db))))
	mux.Handle("/uam/permissions/requests/role-summary", midUAM(http.HandlerFunc(permissions.GetRolePermissionAuditTable(db))))

	server := &http.Server{
		Addr:    ":" + port,
		Handler: observability.WrapHTTP(serviceName, mux),
	}
	return server, pgxPool, nil
}

func StartUAMService(db *sql.DB, port string) {
	server, pool, err := NewUAMServer(db, port)
	if err != nil {
		logger.LogError("UAM Service failed: %v", err)
		return
	}
	defer pool.Close()

	logger.LogInfo("UAM Service started on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.LogError("UAM Service failed: %v", err)
	}
}
