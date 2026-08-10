package dmsjobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ApproveAdhocDispatch applies a pending adhoc request: optional email dispatch,
// then marks run SUCCESS. Stage-then-apply — reject only flips statuses.
func ApproveAdhocDispatch(ctx context.Context, pool *pgxpool.Pool, requestID, checkerBy, comment string) error {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return fmt.Errorf("request_id required")
	}
	var (
		runID, status                                                         string
		storeS3, storeLocal, sendEmail                                        bool
		moduleCode, subModule, emailTplID, emailSubj, emailBody, requestedBy string
		emailTo, emailCc                                                      []string
		pkg, outputFormat                                                     string
	)
	err := pool.QueryRow(ctx, `
		SELECT run_id::text, processing_status, store_s3, COALESCE(store_local, false), send_email, package_mode,
		       module_code, sub_module_code, COALESCE(email_template_id::text,''),
		       email_to, email_cc, email_subject, email_body_html, requested_by,
		       COALESCE(output_format, '')
		FROM dms_svc.adhoc_dispatch_request
		WHERE request_id = $1::uuid AND is_deleted = false`, requestID,
	).Scan(&runID, &status, &storeS3, &storeLocal, &sendEmail, &pkg,
		&moduleCode, &subModule, &emailTplID,
		&emailTo, &emailCc, &emailSubj, &emailBody, &requestedBy, &outputFormat)
	if err != nil {
		return fmt.Errorf("load adhoc request: %w", err)
	}
	if status != "PENDING_APPROVAL" {
		return fmt.Errorf("request %s is %s (expected PENDING_APPROVAL)", requestID, status)
	}

	if sendEmail {
		req := AdhocRequest{
			ModuleCode:      moduleCode,
			SubModuleCode:   subModule,
			TriggeredBy:     requestedBy,
			SendEmail:       true,
			PackageMode:     pkg,
			EmailTemplateID: emailTplID,
			EmailTo:         emailTo,
			EmailCc:         emailCc,
			EmailSubject:    emailSubj,
			EmailBodyHTML:   emailBody,
		}
		// Prefer subject/body already on generation_run (cover rendered at create).
		var storedSubj, storedBody string
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(email_subject,''), COALESCE(email_body_html,'')
			FROM dms_svc.generation_run WHERE run_id = $1::uuid`, runID,
		).Scan(&storedSubj, &storedBody)
		if storedSubj != "" {
			req.EmailSubject = storedSubj
		}
		if storedBody != "" {
			req.EmailBodyHTML = storedBody
		}
		if err := DispatchAdhocWithRecipients(ctx, pool, runID, req); err != nil {
			api.LogError("[DMS-ADHOC] approve email run=%s: %v", runID, err)
			return fmt.Errorf("email dispatch: %w", err)
		}
	} else if storeS3 || storeLocal {
		_, _ = pool.Exec(ctx, `
			UPDATE dms_svc.generated_document SET status = 'DISPATCHED'
			WHERE run_id = $1::uuid AND status = 'GENERATED'`, runID)
	}

	_, err = pool.Exec(ctx, `
		UPDATE dms_svc.adhoc_dispatch_request
		SET processing_status = 'APPROVED', checker_by = $2, checker_at = now(),
		    checker_comment = NULLIF($3,'')
		WHERE request_id = $1::uuid`, requestID, checkerBy, comment)
	if err != nil {
		return err
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO dms_svc.adhoc_dispatch_request_audit (
			request_id, run_id, action_type, processing_status, reason, requested_by,
			checker_by, checker_at, checker_comment,
			old_processing_status, new_processing_status, old_send_email, new_send_email,
			old_store_s3, new_store_s3, old_store_local, new_store_local,
			old_output_format, new_output_format, old_package_mode, new_package_mode,
			old_module_code, new_module_code, old_sub_module_code, new_sub_module_code
		) VALUES ($1::uuid, $2::uuid, 'APPROVE', 'APPROVED', 'Approved adhoc dispatch', $3,
			$4, now(), NULLIF($5,''),
			'PENDING_APPROVAL', 'APPROVED', $6, $6, $7, $7, $8, $8,
			$9, $9, $10, $10, $11, $11, $12, $12)`,
		requestID, runID, requestedBy, checkerBy, comment, sendEmail, storeS3, storeLocal,
		outputFormat, pkg, moduleCode, subModule)

	return finishRun(ctx, pool, runID, "SUCCESS", "")
}

// RejectAdhocDispatch marks the pending request rejected; docs stay on S3 as GENERATED.
func RejectAdhocDispatch(ctx context.Context, pool *pgxpool.Pool, requestID, checkerBy, comment string) error {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return fmt.Errorf("request_id required")
	}
	var runID, status, requestedBy, moduleCode, subModule, pkg, outputFormat string
	var storeS3, storeLocal, sendEmail bool
	err := pool.QueryRow(ctx, `
		SELECT run_id::text, processing_status, store_s3, COALESCE(store_local, false), send_email, requested_by,
		       module_code, sub_module_code, package_mode, COALESCE(output_format, '')
		FROM dms_svc.adhoc_dispatch_request
		WHERE request_id = $1::uuid AND is_deleted = false`, requestID,
	).Scan(&runID, &status, &storeS3, &storeLocal, &sendEmail, &requestedBy,
		&moduleCode, &subModule, &pkg, &outputFormat)
	if err != nil {
		return fmt.Errorf("load adhoc request: %w", err)
	}
	if status != "PENDING_APPROVAL" {
		return fmt.Errorf("request %s is %s (expected PENDING_APPROVAL)", requestID, status)
	}
	_, err = pool.Exec(ctx, `
		UPDATE dms_svc.adhoc_dispatch_request
		SET processing_status = 'REJECTED', checker_by = $2, checker_at = now(),
		    checker_comment = NULLIF($3,'')
		WHERE request_id = $1::uuid`, requestID, checkerBy, comment)
	if err != nil {
		return err
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO dms_svc.adhoc_dispatch_request_audit (
			request_id, run_id, action_type, processing_status, reason, requested_by,
			checker_by, checker_at, checker_comment,
			old_processing_status, new_processing_status, old_send_email, new_send_email,
			old_store_s3, new_store_s3, old_store_local, new_store_local,
			old_output_format, new_output_format, old_package_mode, new_package_mode,
			old_module_code, new_module_code, old_sub_module_code, new_sub_module_code
		) VALUES ($1::uuid, $2::uuid, 'REJECT', 'REJECTED', 'Rejected adhoc dispatch', $3,
			$4, now(), NULLIF($5,''),
			'PENDING_APPROVAL', 'REJECTED', $6, $6, $7, $7, $8, $8,
			$9, $9, $10, $10, $11, $11, $12, $12)`,
		requestID, runID, requestedBy, checkerBy, comment, sendEmail, storeS3, storeLocal,
		outputFormat, pkg, moduleCode, subModule)
	return finishRun(ctx, pool, runID, "REJECTED", strings.TrimSpace(comment))
}

// ListPendingAdhocRequests returns pending maker-checker adhoc rows.
// Optional moduleCode / subModuleCode scopes the queue for a Generate workspace.
func ListPendingAdhocRequests(
	ctx context.Context,
	pool *pgxpool.Pool,
	limit int,
	moduleCode, subModuleCode string,
) ([]map[string]any, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	args := []any{limit}
	where := `r.is_deleted = false AND r.processing_status = 'PENDING_APPROVAL'`
	if m := strings.TrimSpace(moduleCode); m != "" {
		args = append(args, m)
		where += fmt.Sprintf(` AND r.module_code = $%d`, len(args))
	}
	if s := strings.TrimSpace(subModuleCode); s != "" {
		args = append(args, s)
		where += fmt.Sprintf(` AND r.sub_module_code = $%d`, len(args))
	}
	q := fmt.Sprintf(`
		SELECT r.request_id::text, r.run_id::text, r.module_code, r.sub_module_code,
		       r.output_format, r.store_s3, COALESCE(r.store_local, false), r.send_email, r.package_mode,
		       r.email_to, r.email_cc, COALESCE(r.email_subject,''), r.requested_by, r.created_at,
		       r.processing_status,
		       COALESCE(r.document_template_id::text,''),
		       COALESCE((
		         SELECT array_agg(s.source_id ORDER BY s.sort_order)
		         FROM dms_svc.generation_run_source_row s WHERE s.run_id = r.run_id
		       ), '{}') AS source_ids,
		       COALESCE((
		         SELECT array_agg(d.output_filename ORDER BY d.created_at)
		         FROM dms_svc.generated_document d WHERE d.run_id = r.run_id
		       ), '{}') AS filenames,
		       COALESCE((
		         SELECT COUNT(*)::int FROM dms_svc.generated_document d WHERE d.run_id = r.run_id
		       ), 0) AS doc_count
		FROM dms_svc.adhoc_dispatch_request r
		WHERE %s
		ORDER BY r.created_at DESC
		LIMIT $1`, where)
	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var (
			reqID, runID, mod, sub, fmtOut, pkg, by, status, tplID, subj string
			store, storeLocal, send                                     bool
			to, cc, sourceIDs, filenames                                []string
			created                                                     interface{}
			docCount                                                    int
		)
		if err := rows.Scan(
			&reqID, &runID, &mod, &sub, &fmtOut, &store, &storeLocal, &send, &pkg,
			&to, &cc, &subj, &by, &created, &status, &tplID,
			&sourceIDs, &filenames, &docCount,
		); err != nil {
			return nil, err
		}
		if to == nil {
			to = []string{}
		}
		if cc == nil {
			cc = []string{}
		}
		if sourceIDs == nil {
			sourceIDs = []string{}
		}
		if filenames == nil {
			filenames = []string{}
		}
		out = append(out, map[string]any{
			"request_id":           reqID,
			"run_id":               runID,
			"module_code":          mod,
			"sub_module_code":      sub,
			"output_format":        fmtOut,
			"store_s3":             store,
			"store_local":          storeLocal,
			"send_email":           send,
			"package_mode":         pkg,
			"email_to":             to,
			"email_cc":             cc,
			"email_subject":        subj,
			"requested_by":         by,
			"created_at":           created,
			"processing_status":    status,
			"document_template_id": tplID,
			"source_ids":           sourceIDs,
			"filenames":            filenames,
			"doc_count":            docCount,
			"destination":          destinationLabel(store, storeLocal, send),
			"source_ids_label":     strings.Join(sourceIDs, ", "),
			"filenames_label":      strings.Join(filenames, ", "),
			"email_to_label":       strings.Join(to, ", "),
		})
	}
	return out, rows.Err()
}

func destinationLabel(store, storeLocal, send bool) string {
	parts := make([]string, 0, 3)
	if store {
		parts = append(parts, "S3")
	}
	if storeLocal {
		parts = append(parts, "Local")
	}
	if send {
		parts = append(parts, "Email")
	}
	if len(parts) == 0 {
		return "—"
	}
	return strings.Join(parts, " · ")
}

// ListAdhocAuditLog returns adhoc_dispatch_request_audit rows (newest first)
// with old_/new_ pairs for AuditTrailSection Change Summary.
func ListAdhocAuditLog(
	ctx context.Context,
	pool *pgxpool.Pool,
	requestID, moduleCode, subModuleCode string,
	limit int,
) ([]map[string]any, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	args := []any{limit}
	where := `1=1`
	if id := strings.TrimSpace(requestID); id != "" {
		args = append(args, id)
		where += fmt.Sprintf(` AND a.request_id = $%d::uuid`, len(args))
	}
	if m := strings.TrimSpace(moduleCode); m != "" {
		args = append(args, m)
		where += fmt.Sprintf(` AND COALESCE(a.new_module_code, a.old_module_code, r.module_code) = $%d`, len(args))
	}
	if s := strings.TrimSpace(subModuleCode); s != "" {
		args = append(args, s)
		where += fmt.Sprintf(` AND COALESCE(a.new_sub_module_code, a.old_sub_module_code, r.sub_module_code) = $%d`, len(args))
	}
	q := fmt.Sprintf(`
		SELECT a.audit_id::text, a.request_id::text, a.run_id::text,
		       a.action_type, a.processing_status,
		       COALESCE(a.reason, '') AS reason,
		       COALESCE(a.requested_by, '') AS requested_by,
		       a.created_at AS requested_at,
		       COALESCE(a.checker_by, '') AS checker_by,
		       a.checker_at,
		       COALESCE(a.checker_comment, '') AS checker_comment,
		       a.old_processing_status, a.new_processing_status,
		       a.old_send_email, a.new_send_email,
		       a.old_store_s3, a.new_store_s3,
		       a.old_store_local, a.new_store_local,
		       a.old_output_format, a.new_output_format,
		       a.old_package_mode, a.new_package_mode,
		       a.old_module_code, a.new_module_code,
		       a.old_sub_module_code, a.new_sub_module_code
		FROM dms_svc.adhoc_dispatch_request_audit a
		LEFT JOIN dms_svc.adhoc_dispatch_request r ON r.request_id = a.request_id
		WHERE %s
		ORDER BY a.created_at DESC
		LIMIT $1`, where)
	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var (
			auditID, reqID, runID, action, proc, reason, by, checkerBy, checkerComment string
			requestedAt                                                                interface{}
			checkerAt                                                                  *time.Time
			oldProc, newProc                                                           *string
			oldSend, newSend, oldS3, newS3, oldLocal, newLocal                         *bool
			oldFmt, newFmt, oldPkg, newPkg, oldMod, newMod, oldSub, newSub             *string
		)
		if err := rows.Scan(
			&auditID, &reqID, &runID, &action, &proc, &reason, &by, &requestedAt,
			&checkerBy, &checkerAt, &checkerComment,
			&oldProc, &newProc, &oldSend, &newSend, &oldS3, &newS3, &oldLocal, &newLocal,
			&oldFmt, &newFmt, &oldPkg, &newPkg, &oldMod, &newMod, &oldSub, &newSub,
		); err != nil {
			return nil, err
		}
		row := map[string]any{
			"audit_id":              auditID,
			"request_id":            reqID,
			"run_id":                runID,
			"action_type":           action,
			"processing_status":     proc,
			"reason":                reason,
			"requested_by":          by,
			"requested_at":          requestedAt,
			"checker_by":            checkerBy,
			"checker_comment":       checkerComment,
			"old_processing_status": oldProc,
			"new_processing_status": newProc,
			"old_send_email":        oldSend,
			"new_send_email":        newSend,
			"old_store_s3":          oldS3,
			"new_store_s3":          newS3,
			"old_store_local":       oldLocal,
			"new_store_local":       newLocal,
			"old_output_format":     oldFmt,
			"new_output_format":     newFmt,
			"old_package_mode":      oldPkg,
			"new_package_mode":      newPkg,
			"old_module_code":       oldMod,
			"new_module_code":       newMod,
			"old_sub_module_code":   oldSub,
			"new_sub_module_code":   newSub,
		}
		if checkerAt != nil {
			row["checker_at"] = checkerAt.UTC().Format(time.RFC3339)
		} else {
			row["checker_at"] = nil
		}
		switch t := requestedAt.(type) {
		case time.Time:
			row["requested_at"] = t.UTC().Format(time.RFC3339)
		case *time.Time:
			if t != nil {
				row["requested_at"] = t.UTC().Format(time.RFC3339)
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
