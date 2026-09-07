package evidencePack

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	s3storage "CimplrCorpSaas/api/utils/s3storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MaterializeEvidencePack builds a ZIP of closing evidence section reports from
// live cycle/checklist/lock/audit data, uploads it to S3, and stamps
// s3_key/file_size/checksum/document_count on the pack row.
//
// Why this exists: FireDmsEvent is a silent no-op until a dms_svc.generation_rule
// for FD_CLOSING_EVIDENCE_PACK is seeded. Demo / local flows need a real
// downloadable artifact without waiting on DMS.
func MaterializeEvidencePack(ctx context.Context, pool *pgxpool.Pool, packID string) (string, error) {
	packID = strings.TrimSpace(packID)
	if packID == "" {
		return "", fmt.Errorf("pack_id is required")
	}

	var (
		cycleID                                            string
		format                                             string
		incAccrual, incRecon, incExceptions, incPosting    bool
		incApprovals, incLockCert, incAudit, incSupporting bool
		existingKey                                        string
	)
	err := pool.QueryRow(ctx, `
		SELECT cycle_id, format,
		       include_accrual_ledger, include_reconciliation_report, include_exceptions_register,
		       include_posting_summary, include_approval_logs, include_period_lock_certificate,
		       include_audit_trail, include_supporting_documents,
		       COALESCE(s3_key,'')
		FROM investment.fd_closing_evidence_pack
		WHERE pack_id = $1 AND is_deleted = false`,
		packID,
	).Scan(
		&cycleID, &format,
		&incAccrual, &incRecon, &incExceptions, &incPosting,
		&incApprovals, &incLockCert, &incAudit, &incSupporting,
		&existingKey,
	)
	if err != nil {
		return "", fmt.Errorf("load pack: %w", err)
	}
	if strings.TrimSpace(existingKey) != "" {
		return existingKey, nil
	}

	var (
		entityID, entityName, closeType, financialPeriod, status, eligibility string
		periodStart, periodEnd                                                time.Time
		fdCount                                                               int
		readiness                                                             float64
		initiatedBy                                                           string
	)
	if err := pool.QueryRow(ctx, `
		SELECT entity_id, entity_name, close_type, financial_period,
		       period_start, period_end, status, eligibility,
		       COALESCE(fd_count,0), COALESCE(readiness_score,0), COALESCE(initiated_by,'')
		FROM investment.fd_closing_cycle
		WHERE cycle_id = $1 AND is_deleted = false`,
		cycleID,
	).Scan(
		&entityID, &entityName, &closeType, &financialPeriod,
		&periodStart, &periodEnd, &status, &eligibility,
		&fdCount, &readiness, &initiatedBy,
	); err != nil {
		return "", fmt.Errorf("load cycle: %w", err)
	}

	type sectionFile struct {
		Name string
		Body string
	}
	files := []sectionFile{
		{
			Name: "00_cover_summary.txt",
			Body: strings.Join([]string{
				"FD MONTH / QUARTER END CLOSING — EVIDENCE PACK",
				"================================================",
				fmt.Sprintf("Pack ID:           %s", packID),
				fmt.Sprintf("Cycle ID:          %s", cycleID),
				fmt.Sprintf("Entity:            %s (%s)", entityName, entityID),
				fmt.Sprintf("Close Type:        %s", closeType),
				fmt.Sprintf("Financial Period:  %s", financialPeriod),
				fmt.Sprintf("Period Window:     %s → %s", periodStart.Format("2006-01-02"), periodEnd.Format("2006-01-02")),
				fmt.Sprintf("Cycle Status:      %s", status),
				fmt.Sprintf("Eligibility:       %s", eligibility),
				fmt.Sprintf("FDs in Scope:      %d", fdCount),
				fmt.Sprintf("Readiness Score:   %.2f%%", readiness),
				fmt.Sprintf("Initiated By:      %s", initiatedBy),
				fmt.Sprintf("Generated At:      %s IST", time.Now().In(time.FixedZone("IST", 5*3600+30*60)).Format("2006-01-02 15:04:05")),
				"",
				"This pack was materialised by the closing module (DMS-independent",
				"fallback) so demo / local environments can download evidence without",
				"a seeded dms_svc.generation_rule for FD_CLOSING_EVIDENCE_PACK.",
			}, "\n") + "\n",
		},
	}

	add := func(included bool, name, body string) {
		if included {
			files = append(files, sectionFile{Name: name, Body: body})
		}
	}

	if incAccrual {
		rows, qErr := pool.Query(ctx, `
			SELECT COALESCE(run_id,''), COALESCE(run_mode,''), COALESCE(run_status,''),
			       COALESCE(financial_period,''),
			       COALESCE(TO_CHAR(accrual_period_start,'YYYY-MM-DD'),''),
			       COALESCE(TO_CHAR(accrual_period_end,'YYYY-MM-DD'),'')
			FROM investment.fd_accrual_run
			WHERE entity_id = $1
			  AND COALESCE(is_deleted,false) = false
			  AND accrual_period_start <= $3::date
			  AND accrual_period_end >= $2::date
			ORDER BY created_at DESC
			LIMIT 50`,
			entityID, periodStart, periodEnd,
		)
		body := "ACCRUAL LEDGER REPORT\n=====================\n\n"
		if qErr != nil {
			body += "ERROR loading accrual runs: " + qErr.Error() + "\n"
		} else {
			n := 0
			for rows.Next() {
				var runID, mode, st, fp, ps, pe string
				_ = rows.Scan(&runID, &mode, &st, &fp, &ps, &pe)
				n++
				body += fmt.Sprintf("%d. %s | mode=%s | status=%s | period=%s (%s→%s)\n",
					n, runID, mode, st, fp, ps, pe)
			}
			rows.Close()
			if n == 0 {
				body += "(No overlapping accrual runs found for this entity/period.)\n"
			}
		}
		cRows, _ := pool.Query(ctx, `
			SELECT fd_id, step_code, status, COALESCE(evidence_ref,''), COALESCE(evidence_type,'')
			FROM investment.fd_closing_checklist_item
			WHERE cycle_id = $1 AND step_code IN ('ACCRUAL_RUN_COMPLETED','ACCRUAL_RUN_APPROVED')
			ORDER BY fd_id, sequence`, cycleID)
		if cRows != nil {
			body += "\nChecklist accrual evidence\n--------------------------\n"
			for cRows.Next() {
				var fd, step, st, ref, typ string
				_ = cRows.Scan(&fd, &step, &st, &ref, &typ)
				body += fmt.Sprintf("%s | %s | %s | evidence=%s (%s)\n", fd, step, st, ref, typ)
			}
			cRows.Close()
		}
		add(true, "01_accrual_ledger_report.txt", body)
	}

	if incRecon {
		body := "RECONCILIATION REPORT\n=====================\n\n"
		rows, qErr := pool.Query(ctx, `
			SELECT fd_id, step_code, status, COALESCE(evidence_ref,''), COALESCE(exception_count,0)
			FROM investment.fd_closing_checklist_item
			WHERE cycle_id = $1 AND step_code IN ('RECEIPTS_CAPTURED','RECEIPTS_RECONCILED')
			ORDER BY fd_id, sequence`, cycleID)
		if qErr != nil {
			body += "ERROR: " + qErr.Error() + "\n"
		} else {
			for rows.Next() {
				var fd, step, st, ref string
				var ex int
				_ = rows.Scan(&fd, &step, &st, &ref, &ex)
				body += fmt.Sprintf("%s | %s | %s | evidence=%s | exceptions=%d\n", fd, step, st, ref, ex)
			}
			rows.Close()
		}
		add(true, "02_reconciliation_report.txt", body)
	}

	if incExceptions {
		body := "EXCEPTIONS REGISTER\n===================\n\n"
		rows, qErr := pool.Query(ctx, `
			SELECT fd_id, step_code, status, COALESCE(exception_count,0), COALESCE(blocked_comment,'')
			FROM investment.fd_closing_checklist_item
			WHERE cycle_id = $1 AND (COALESCE(exception_count,0) > 0 OR status = 'BLOCKED')
			ORDER BY fd_id, sequence`, cycleID)
		if qErr != nil {
			body += "ERROR: " + qErr.Error() + "\n"
		} else {
			n := 0
			for rows.Next() {
				var fd, step, st, comment string
				var ex int
				_ = rows.Scan(&fd, &step, &st, &ex, &comment)
				n++
				body += fmt.Sprintf("%d. %s | %s | %s | exceptions=%d | %s\n", n, fd, step, st, ex, comment)
			}
			rows.Close()
			if n == 0 {
				body += "(No exceptions / blocked checklist items.)\n"
			}
		}
		add(true, "03_exceptions_register.txt", body)
	}

	if incPosting {
		body := "POSTING SUMMARY\n===============\n\n"
		body += fmt.Sprintf("Cycle %s | status=%s | readiness=%.2f%% | eligibility=%s\n",
			cycleID, status, readiness, eligibility)
		rows, _ := pool.Query(ctx, `
			SELECT step_code,
			       COUNT(*) FILTER (WHERE status='COMPLETED') AS completed,
			       COUNT(*) AS total
			FROM investment.fd_closing_checklist_item
			WHERE cycle_id = $1
			GROUP BY step_code
			ORDER BY MIN(sequence)`, cycleID)
		if rows != nil {
			for rows.Next() {
				var step string
				var completed, total int
				_ = rows.Scan(&step, &completed, &total)
				body += fmt.Sprintf("  %-28s  %d / %d completed\n", step, completed, total)
			}
			rows.Close()
		}
		add(true, "04_posting_summary.txt", body)
	}

	if incApprovals {
		body := "APPROVAL LOGS\n=============\n\n"
		rows, qErr := pool.Query(ctx, `
			SELECT action_type, processing_status, COALESCE(requested_by,''),
			       COALESCE(TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),''),
			       COALESCE(checker_by,''),
			       COALESCE(TO_CHAR((checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),''),
			       COALESCE(reason,''), COALESCE(checker_comment,'')
			FROM investment.fd_closing_cycle_audit
			WHERE cycle_id = $1
			ORDER BY requested_at`, cycleID)
		if qErr != nil {
			body += "ERROR: " + qErr.Error() + "\n"
		} else {
			for rows.Next() {
				var action, ps, reqBy, reqAt, chkBy, chkAt, reason, comment string
				_ = rows.Scan(&action, &ps, &reqBy, &reqAt, &chkBy, &chkAt, &reason, &comment)
				body += fmt.Sprintf("%s | %s | by=%s @ %s | checker=%s @ %s | reason=%s | comment=%s\n",
					action, ps, reqBy, reqAt, chkBy, chkAt, reason, comment)
			}
			rows.Close()
		}
		add(true, "05_approval_logs.txt", body)
	}

	if incLockCert {
		body := "PERIOD LOCK CERTIFICATE\n=======================\n\n"
		rows, qErr := pool.Query(ctx, `
			SELECT request_id, lock_type, processing_status,
			       COALESCE(TO_CHAR(lock_effective_date,'YYYY-MM-DD'),''),
			       COALESCE(requested_by,''),
			       COALESCE(TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),''),
			       COALESCE(checker_by,''),
			       COALESCE(TO_CHAR((checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),''),
			       COALESCE(TO_CHAR((applied_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),''),
			       COALESCE(applied_by,''),
			       COALESCE(remarks,'')
			FROM investment.fd_closing_lock_request
			WHERE cycle_id = $1 AND COALESCE(is_deleted,false)=false
			ORDER BY requested_at`, cycleID)
		if qErr != nil {
			body += "ERROR: " + qErr.Error() + "\n"
		} else {
			n := 0
			for rows.Next() {
				var id, lt, ps, eff, reqBy, reqAt, chkBy, chkAt, appAt, appBy, remarks string
				_ = rows.Scan(&id, &lt, &ps, &eff, &reqBy, &reqAt, &chkBy, &chkAt, &appAt, &appBy, &remarks)
				n++
				body += fmt.Sprintf("%d. %s | %s | %s | effective=%s\n   requested by %s @ %s\n   checker %s @ %s\n   applied by %s @ %s\n   remarks: %s\n\n",
					n, id, lt, ps, eff, reqBy, reqAt, chkBy, chkAt, appBy, appAt, remarks)
			}
			rows.Close()
			if n == 0 {
				body += "(No lock requests on this cycle.)\n"
			}
		}
		body += fmt.Sprintf("\nCertified cycle status at pack time: %s\n", status)
		add(true, "06_period_lock_certificate.txt", body)
	}

	if incAudit {
		body := "AUDIT TRAIL\n===========\n\n"
		rows, qErr := pool.Query(ctx, `
			SELECT event_type, COALESCE(lock_type,''), COALESCE(reason,''), COALESCE(performed_by,''),
			       COALESCE(TO_CHAR((performed_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'')
			FROM investment.fd_closing_cycle_event_log
			WHERE cycle_id = $1
			ORDER BY performed_at`, cycleID)
		if qErr != nil {
			body += "ERROR: " + qErr.Error() + "\n"
		} else {
			n := 0
			for rows.Next() {
				var et, lt, reason, by, at string
				_ = rows.Scan(&et, &lt, &reason, &by, &at)
				n++
				body += fmt.Sprintf("%d. %s | lock=%s | by=%s @ %s | %s\n", n, et, lt, by, at, reason)
			}
			rows.Close()
			if n == 0 {
				body += "(No cycle event log rows.)\n"
			}
		}
		add(true, "07_audit_trail.txt", body)
	}

	if incSupporting {
		body := "SUPPORTING DOCUMENTS INDEX\n==========================\n\n"
		body += "Checklist item evidence refs (system IDs / docs referenced as evidence):\n\n"
		rows, qErr := pool.Query(ctx, `
			SELECT fd_id, step_code, status, COALESCE(evidence_type,''), COALESCE(evidence_ref,'')
			FROM investment.fd_closing_checklist_item
			WHERE cycle_id = $1 AND COALESCE(evidence_ref,'') <> ''
			ORDER BY fd_id, sequence`, cycleID)
		if qErr != nil {
			body += "ERROR: " + qErr.Error() + "\n"
		} else {
			n := 0
			for rows.Next() {
				var fd, step, st, typ, ref string
				_ = rows.Scan(&fd, &step, &st, &typ, &ref)
				n++
				body += fmt.Sprintf("%d. %s | %s | %s | type=%s | ref=%s\n", n, fd, step, st, typ, ref)
			}
			rows.Close()
			if n == 0 {
				body += "(No evidence_ref values on checklist items.)\n"
			}
		}
		add(true, "08_supporting_documents_index.txt", body)
	}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, f := range files {
		w, zErr := zw.Create(f.Name)
		if zErr != nil {
			_ = zw.Close()
			return "", fmt.Errorf("zip create %s: %w", f.Name, zErr)
		}
		if _, zErr = w.Write([]byte(f.Body)); zErr != nil {
			_ = zw.Close()
			return "", fmt.Errorf("zip write %s: %w", f.Name, zErr)
		}
	}
	if err := zw.Close(); err != nil {
		return "", fmt.Errorf("zip close: %w", err)
	}
	payload := buf.Bytes()
	sum := sha256.Sum256(payload)
	checksum := hex.EncodeToString(sum[:])

	s3Key := fmt.Sprintf("fd-closing/evidence-packs/%s/%s_%s.zip",
		cycleID, packID, time.Now().UTC().Format("20060102T150405Z"))
	if err := s3storage.PutObjectToS3(ctx, s3Key, payload, "application/zip"); err != nil {
		return "", fmt.Errorf("s3 upload: %w", err)
	}

	reportCount := len(files)
	if _, err := pool.Exec(ctx, `
		UPDATE investment.fd_closing_evidence_pack
		SET s3_key = $2,
		    file_size = $3,
		    checksum = $4,
		    report_count = $5,
		    document_count = $5,
		    page_count = $5
		WHERE pack_id = $1`,
		packID, s3Key, len(payload), checksum, reportCount,
	); err != nil {
		return "", fmt.Errorf("stamp pack row: %w", err)
	}

	api.LogInfo("[FDClosingEvidencePack] materialised pack=%s key=%s bytes=%d docs=%d (requested_format=%s)",
		packID, s3Key, len(payload), reportCount, format)
	return s3Key, nil
}
