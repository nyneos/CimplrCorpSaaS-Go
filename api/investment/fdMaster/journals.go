package fdMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	accountingworkbench "CimplrCorpSaas/api/investment/accountingWorkbench"
	fdAccounting "CimplrCorpSaas/api/investment/fdAccounting"
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
	return date.Format(constants.DateFormatYearMonth)
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

// buildJournalEntries builds the FD_ACTIVATION journal. When an ACTIVE GL
// mapping exists for (entity, bank, FD_ACTIVATION) it drives the lines and
// dimensions; otherwise the built-in Dr FD Investment / Cr Bank pair is used.
func buildJournalEntries(ctx context.Context, exec queryExecutor, fd *FDRecord, bankInfo *accountingworkbench.BankAccountInfo, activityID string) ([]*accountingworkbench.JournalEntry, error) {
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

	narration := fmt.Sprintf(constants.FormatFDActivation, fd.FDID)
	mapped, mapErr := fdAccounting.BuildMappedJournal(ctx, exec, fd.EntityID, fd.BankID, "FD_ACTIVATION",
		fdAccounting.AmountSet{Full: amount}, fmt.Sprintf("| fd_id=%s", fd.FDID))
	if mapErr != nil {
		return nil, mapErr
	}

	je := &accountingworkbench.JournalEntry{
		ActivityID:       activityID,
		EntityID:         fd.EntityID,
		EntityName:       entityName,
		EntryDate:        entryDate,
		AccountingPeriod: buildAccountingPeriod(entryDate),
		EntryType:        "FD_ACTIVATION",
		Description:      narration,
		TotalDebit:       amount,
		TotalCredit:      amount,
	}

	if mapped.Mapped {
		je.TotalDebit = mapped.Debit
		je.TotalCredit = mapped.Credit
		je.GlMappingVersion = mapped.Version
		je.Lines = make([]accountingworkbench.JournalEntryLine, 0, len(mapped.Lines))
		for _, l := range mapped.Lines {
			je.Lines = append(je.Lines, accountingworkbench.JournalEntryLine{
				LineNumber:    l.LineNumber,
				AccountNumber: l.AccountNumber,
				AccountName:   l.AccountName,
				AccountType:   l.AccountType,
				DebitAmount:   l.Debit,
				CreditAmount:  l.Credit,
				Narration:     l.Narration,
				CostCenter:    l.CostCenter,
				ProfitCenter:  l.ProfitCenter,
				ProjectCode:   l.ProjectCode,
				TaxCode:       l.TaxCode,
			})
		}
	} else {
		je.Lines = []accountingworkbench.JournalEntryLine{
			{
				LineNumber:    1,
				AccountNumber: "FD_INVESTMENT",
				AccountName:   "Fixed Deposit Investment",
				AccountType:   "ASSET",
				DebitAmount:   amount,
				CreditAmount:  0,
				Narration:     narration,
			},
			{
				LineNumber:    2,
				AccountNumber: bankAccountNumber,
				AccountName:   bankAccountName,
				AccountType:   "ASSET",
				DebitAmount:   0,
				CreditAmount:  amount,
				Narration:     narration,
			},
		}
	}

	return []*accountingworkbench.JournalEntry{je}, nil
}

func CreateFDAccountingActivity(ctx context.Context, exec queryExecutor, fdID string, effectiveDate time.Time, userEmail string) (string, error) {
	var activityID string
	err := exec.QueryRow(ctx, `
		INSERT INTO investment.accounting_activity (
			activity_type, activity_subtype, effective_date, accounting_period, data_source, status
		) VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING activity_id
	`, "FIXED_DEPOSIT", "ACTIVATION", effectiveDate, buildAccountingPeriod(effectiveDate), "FD_MASTER", constants.StatusPendingApproval).Scan(&activityID)
	if err != nil {
		return "", fmt.Errorf("create accounting activity: %w", err)
	}

	// Activity mirrors the journal: pending until FD Accounting Workbench approve/post.
	if _, err := exec.Exec(ctx, `
		INSERT INTO investment.auditactionaccountingactivity (
			activity_id, actiontype, processing_status, requested_by, requested_at, requested_ip
		) VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now(), $3)
	`, activityID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
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
				accounting_period, entry_type, description, total_debit, total_credit,
				gl_mapping_version, status, fd_id, created_by
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULLIF($12,''),'PENDING_APPROVAL',$13,$14)
			RETURNING entry_id
		`, je.ActivityID, je.EntityID, je.EntityName, je.FolioID, je.DematID, je.EntryDate,
			je.AccountingPeriod, je.EntryType, fmt.Sprintf("%s | fd_id=%s", je.Description, fdID),
			je.TotalDebit, je.TotalCredit, je.GlMappingVersion, fdID, userEmail,
		).Scan(&entryID)
		if err != nil {
			return fmt.Errorf("insert journal entry: %w", err)
		}
		if err := fdAccounting.StageJournalForApproval(ctx, exec, entryID, userEmail, "FD activation journal"); err != nil {
			return err
		}

		for _, line := range je.Lines {
			if _, err := exec.Exec(ctx, `
				INSERT INTO investment.accounting_journal_entry_line (
					entry_id, line_number, account_number, account_name, account_type,
					debit_amount, credit_amount, scheme_id, folio_id, demat_id, narration,
					cost_center, profit_center, project_code, tax_code
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULLIF($12,''),NULLIF($13,''),NULLIF($14,''),NULLIF($15,''))
			`, entryID, line.LineNumber, line.AccountNumber, line.AccountName, line.AccountType,
				line.DebitAmount, line.CreditAmount, line.SchemeID, line.FolioID, line.DematID,
				fmt.Sprintf("%s | fd_id=%s", line.Narration, fdID),
				line.CostCenter, line.ProfitCenter, line.ProjectCode, line.TaxCode,
			); err != nil {
				return fmt.Errorf("insert journal line: %w", err)
			}
		}
	}

	return nil
}
