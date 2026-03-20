package fdMaster

import (
	accountingworkbench "CimplrCorpSaas/api/investment/accountingWorkbench"
	"context"
	"fmt"
	"math"
	"time"
)

type FDJournalEntry struct {
	FDID       string
	ActivityID string
	Entry      *accountingworkbench.JournalEntry
}

func buildAccountingPeriod(date time.Time) string {
	return date.Format("2006-01")
}

func loadBankAccountInfo(ctx context.Context, exec queryExecutor, bankAccountID string) (*accountingworkbench.BankAccountInfo, error) {
	if bankAccountID == "" {
		return nil, fmt.Errorf("bank account id missing")
	}

	var info accountingworkbench.BankAccountInfo
	err := exec.QueryRow(ctx, `
		SELECT
			COALESCE(mba.account_number, mba.account_id::text, ''),
			COALESCE(mba.account_nickname, 'Bank Account'),
			COALESCE(mb.bank_name, ''),
			COALESCE(me.entity_name, mec.entity_name, '')
		FROM public.masterbankaccount mba
		LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
		LEFT JOIN public.masterentity me ON me.entity_id::text = mba.entity_id
		LEFT JOIN public.masterentitycash mec ON mec.entity_id::text = mba.entity_id
		WHERE mba.account_id::text = $1 OR mba.account_number = $1 OR mba.account_nickname = $1
		LIMIT 1
	`, bankAccountID).Scan(
		&info.AccountNumber,
		&info.AccountName,
		&info.BankName,
		&info.EntityName,
	)
	if err != nil {
		return nil, err
	}
	if info.AccountNumber == "" {
		info.AccountNumber = bankAccountID
	}
	if info.AccountName == "" {
		info.AccountName = "Bank Account"
	}
	return &info, nil
}

func buildJournalEntries(fd *FDRecord, bankInfo *accountingworkbench.BankAccountInfo, activityID string) []*accountingworkbench.JournalEntry {
	amount := math.Round(fd.PrincipalAmount*100) / 100
	entryDate := fd.ValueDate
	if entryDate.IsZero() {
		entryDate = time.Now()
	}

	entityName := fd.EntityID
	if bankInfo != nil && bankInfo.EntityName != "" {
		entityName = bankInfo.EntityName
	}

	bankAccountNumber := fd.BankAccountID
	bankAccountName := "Bank Account"
	if bankInfo != nil {
		if bankInfo.AccountNumber != "" {
			bankAccountNumber = bankInfo.AccountNumber
		}
		if bankInfo.AccountName != "" {
			bankAccountName = bankInfo.AccountName
		}
	}

	je := &accountingworkbench.JournalEntry{
		ActivityID:       activityID,
		EntityID:         fd.EntityID,
		EntityName:       entityName,
		EntryDate:        entryDate,
		AccountingPeriod: buildAccountingPeriod(entryDate),
		EntryType:        "FD_ACTIVATION",
		Description:      fmt.Sprintf("FD activation %s", fd.FDID),
		TotalDebit:       amount,
		TotalCredit:      amount,
		Lines: []accountingworkbench.JournalEntryLine{
			{
				LineNumber:    1,
				AccountNumber: "FD_INVESTMENT",
				AccountName:   "Fixed Deposit Investment",
				AccountType:   "ASSET",
				DebitAmount:   amount,
				CreditAmount:  0,
				Narration:     fmt.Sprintf("FD activation %s", fd.FDID),
			},
			{
				LineNumber:    2,
				AccountNumber: bankAccountNumber,
				AccountName:   bankAccountName,
				AccountType:   "ASSET",
				DebitAmount:   0,
				CreditAmount:  amount,
				Narration:     fmt.Sprintf("FD activation %s", fd.FDID),
			},
		},
	}

	return []*accountingworkbench.JournalEntry{je}
}

func CreateFDAccountingActivity(ctx context.Context, exec queryExecutor, fdID string, effectiveDate time.Time, userEmail string) (string, error) {
	var activityID string
	err := exec.QueryRow(ctx, `
		INSERT INTO investment.accounting_activity (
			activity_type, activity_subtype, effective_date, accounting_period, data_source, status
		) VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING activity_id
	`, "FIXED_DEPOSIT", "ACTIVATION", effectiveDate, buildAccountingPeriod(effectiveDate), "FD_MASTER", "APPROVED").Scan(&activityID)
	if err != nil {
		return "", fmt.Errorf("create accounting activity: %w", err)
	}

	if _, err := exec.Exec(ctx, `
		INSERT INTO investment.auditactionaccountingactivity (
			activity_id, actiontype, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment
		) VALUES ($1, 'CREATE', 'APPROVED', $2, now(), $2, now(), $3)
	`, activityID, userEmail, fmt.Sprintf("FD activation %s", fdID)); err != nil {
		return "", fmt.Errorf("create accounting activity audit: %w", err)
	}

	return activityID, nil
}

func SaveFDJournalEntries(ctx context.Context, exec queryExecutor, fdID string, userEmail string, entries []*accountingworkbench.JournalEntry) error {
	for _, je := range entries {
		if je.TotalDebit != je.TotalCredit {
			return fmt.Errorf("journal entry not balanced for fd %s", fdID)
		}

		var entryID string
		err := exec.QueryRow(ctx, `
			INSERT INTO investment.accounting_journal_entry (
				activity_id, entity_id, entity_name, folio_id, demat_id, entry_date,
				accounting_period, entry_type, description, total_debit, total_credit, status, fd_id, created_by
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'POSTED',$12,$13)
			RETURNING entry_id
		`, je.ActivityID, je.EntityID, je.EntityName, je.FolioID, je.DematID, je.EntryDate,
			je.AccountingPeriod, je.EntryType, fmt.Sprintf("%s | fd_id=%s", je.Description, fdID), je.TotalDebit, je.TotalCredit, fdID, userEmail,
		).Scan(&entryID)
		if err != nil {
			return fmt.Errorf("insert journal entry: %w", err)
		}

		for _, line := range je.Lines {
			if _, err := exec.Exec(ctx, `
				INSERT INTO investment.accounting_journal_entry_line (
					entry_id, line_number, account_number, account_name, account_type,
					debit_amount, credit_amount, scheme_id, folio_id, demat_id, narration
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			`, entryID, line.LineNumber, line.AccountNumber, line.AccountName, line.AccountType,
				line.DebitAmount, line.CreditAmount, line.SchemeID, line.FolioID, line.DematID, fmt.Sprintf("%s | fd_id=%s", line.Narration, fdID),
			); err != nil {
				return fmt.Errorf("insert journal line: %w", err)
			}
		}
	}

	return nil
}
