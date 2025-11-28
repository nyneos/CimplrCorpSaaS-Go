package accountingworkbench

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBExecutor is an interface that both pgx.Tx and pgxpool.Pool implement
type DBExecutor interface {
	QueryRow(ctx context.Context, sql string, arguments ...interface{}) pgx.Row
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, arguments ...interface{}) (pgx.Rows, error)
}

// JournalEntryLine represents a single line in a journal entry
type JournalEntryLine struct {
	LineNumber    int
	AccountNumber string // From masterbankaccount.account_number (via folio/demat)
	AccountName   string
	AccountType   string // ASSET, LIABILITY, EQUITY, INCOME, EXPENSE
	DebitAmount   float64
	CreditAmount  float64
	SchemeID      string
	FolioID       string
	DematID       string
	Narration     string
}

// JournalEntry represents a complete journal entry
type JournalEntry struct {
	ActivityID       string
	EntityID         string
	EntityName       string
	FolioID          string
	DematID          string
	EntryDate        time.Time
	AccountingPeriod string
	EntryType        string
	Description      string
	Lines            []JournalEntryLine
	TotalDebit       float64
	TotalCredit      float64
}

// BankAccountInfo holds account details fetched from database
type BankAccountInfo struct {
	AccountNumber string
	AccountName   string
	BankName      string
	EntityName    string
}

// GetBankAccountFromFolio fetches redemption account details from folio
func GetBankAccountFromFolio(ctx context.Context, executor DBExecutor, folioID string) (*BankAccountInfo, error) {
	query := `
		SELECT 
			COALESCE(ba.account_number, '') AS account_number,
			COALESCE(ba.account_nickname, '') AS account_name,
			COALESCE(b.bank_name, '') AS bank_name,
			COALESCE(e.entity_name, ec.entity_name, '') AS entity_name
		FROM investment.masterfolio f
		LEFT JOIN public.masterbankaccount ba ON (
			ba.account_number = f.default_redemption_account 
			OR ba.account_id = f.default_redemption_account
			OR ba.account_nickname = f.default_redemption_account
		)
		LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
		LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
		LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
		WHERE f.folio_id = $1
		LIMIT 1
	`
	
	var info BankAccountInfo
	err := executor.QueryRow(ctx, query, folioID).Scan(
		&info.AccountNumber,
		&info.AccountName,
		&info.BankName,
		&info.EntityName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get bank account for folio %s: %w", folioID, err)
	}
	
	// Validate that we have at least account_number
	if info.AccountNumber == "" {
		return nil, fmt.Errorf("no bank account configured for folio %s", folioID)
	}
	
	return &info, nil
}

// GetBankAccountFromDemat fetches settlement account details from demat
func GetBankAccountFromDemat(ctx context.Context, executor DBExecutor, dematID string) (*BankAccountInfo, error) {
	query := `
		SELECT 
			COALESCE(ba.account_number, '') AS account_number,
			COALESCE(ba.account_nickname, '') AS account_name,
			COALESCE(b.bank_name, '') AS bank_name,
			COALESCE(e.entity_name, ec.entity_name, '') AS entity_name
		FROM investment.masterdemataccount d
		LEFT JOIN public.masterbankaccount ba ON (
			ba.account_number = d.default_settlement_account
			OR ba.account_id = d.default_settlement_account
			OR ba.account_nickname = d.default_settlement_account
		)
		LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
		LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
		LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
		WHERE d.demat_id = $1
		LIMIT 1
	`
	
	var info BankAccountInfo
	err := executor.QueryRow(ctx, query, dematID).Scan(
		&info.AccountNumber,
		&info.AccountName,
		&info.BankName,
		&info.EntityName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get bank account for demat %s: %w", dematID, err)
	}
	
	// Validate that we have at least account_number
	if info.AccountNumber == "" {
		return nil, fmt.Errorf("no bank account configured for demat %s", dematID)
	}
	
	return &info, nil
}

// GenerateJournalEntryForMTM creates journal entry for MTM activity
func GenerateJournalEntryForMTM(ctx context.Context, executor DBExecutor, settings *SettingsCache, activityID string, mtmData map[string]interface{}) (*JournalEntry, error) {
	unrealizedGL := parseFloat(mtmData["unrealized_gain_loss"])
	schemeID := parseString(mtmData["scheme_id"])
	
	// Handle pointer types for folio_id and demat_id
	var folioID, dematID string
	if fid := mtmData["folio_id"]; fid != nil {
		if ptr, ok := fid.(*string); ok && ptr != nil {
			folioID = *ptr
		} else {
			folioID = parseString(fid)
		}
	}
	if did := mtmData["demat_id"]; did != nil {
		if ptr, ok := did.(*string); ok && ptr != nil {
			dematID = *ptr
		} else {
			dematID = parseString(did)
		}
	}

	// Get scheme name from database if not provided
	var schemeName string
	if err := executor.QueryRow(ctx, `SELECT scheme_name FROM investment.masterscheme WHERE scheme_id = $1`, schemeID).Scan(&schemeName); err != nil {
		schemeName = schemeID // fallback to scheme_id
	}

	// Fetch bank account from folio or demat
	var bankAccount *BankAccountInfo
	var err error
	if folioID != "" {
		bankAccount, err = GetBankAccountFromFolio(ctx, executor, folioID)
		if err != nil {
			return nil, fmt.Errorf("failed to get folio bank account: %w", err)
		}
	} else if dematID != "" {
		bankAccount, err = GetBankAccountFromDemat(ctx, executor, dematID)
		if err != nil {
			return nil, fmt.Errorf("failed to get demat bank account: %w", err)
		}
	} else {
		return nil, fmt.Errorf("neither folio_id nor demat_id provided for MTM activity")
	}

	je := &JournalEntry{
		ActivityID:  activityID,
		EntityID:    bankAccount.EntityName, // Use entity from bank account
		EntityName:  bankAccount.EntityName,
		FolioID:     folioID,
		DematID:     dematID,
		EntryDate:   time.Now(),
		Lines:       []JournalEntryLine{},
	}

	roundedAmount := RoundAmount(math.Abs(unrealizedGL), settings.CurrencyPrecision)

	if unrealizedGL > 0 {
		// MTM Gain: DR Investment Account (Bank), CR Unrealized Gain (Income placeholder - 4101)
		je.EntryType = "MTM_GAIN"
		je.Description = fmt.Sprintf("MTM Gain on %s", schemeName)

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    1,
			AccountNumber: bankAccount.AccountNumber,
			AccountName:   bankAccount.AccountName + " - " + bankAccount.BankName,
			AccountType:   "ASSET",
			DebitAmount:   roundedAmount,
			CreditAmount:  0,
			SchemeID:      schemeID,
			FolioID:       folioID,
			DematID:       dematID,
			Narration:     fmt.Sprintf("MTM gain - %s", schemeName),
		})

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    2,
			AccountNumber: "4101", // Unrealized Gain (Income) - standard GL code
			AccountName:   "Unrealized Gain on Investments",
			AccountType:   "INCOME",
			DebitAmount:   0,
			CreditAmount:  roundedAmount,
			SchemeID:      schemeID,
			FolioID:       folioID,
			DematID:       dematID,
			Narration:     fmt.Sprintf("MTM gain - %s", schemeName),
		})

		je.TotalDebit = roundedAmount
		je.TotalCredit = roundedAmount

	} else if unrealizedGL < 0 {
		// MTM Loss: DR Unrealized Loss (Expense - 5101), CR Investment Account (Bank)
		je.EntryType = "MTM_LOSS"
		je.Description = fmt.Sprintf("MTM Loss on %s", schemeName)

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    1,
			AccountNumber: "5101", // Unrealized Loss (Expense) - standard GL code
			AccountName:   "Unrealized Loss on Investments",
			AccountType:   "EXPENSE",
			DebitAmount:   roundedAmount,
			CreditAmount:  0,
			SchemeID:      schemeID,
			FolioID:       folioID,
			DematID:       dematID,
			Narration:     fmt.Sprintf("MTM loss - %s", schemeName),
		})

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    2,
			AccountNumber: bankAccount.AccountNumber,
			AccountName:   bankAccount.AccountName + " - " + bankAccount.BankName,
			AccountType:   "ASSET",
			DebitAmount:   0,
			CreditAmount:  roundedAmount,
			SchemeID:      schemeID,
			FolioID:       folioID,
			DematID:       dematID,
			Narration:     fmt.Sprintf("MTM loss - %s", schemeName),
		})

		je.TotalDebit = roundedAmount
		je.TotalCredit = roundedAmount
	} else {
		// No gain/loss, skip journal entry
		return nil, nil
	}

	return je, nil
}

// GenerateJournalEntryForDividend creates journal entry for dividend activity
func GenerateJournalEntryForDividend(ctx context.Context, executor DBExecutor, settings *SettingsCache, activityID string, dividendData map[string]interface{}) (*JournalEntry, error) {
	transactionType := parseString(dividendData["transaction_type"])
	dividendAmount := parseFloat(dividendData["dividend_amount"])
	schemeID := parseString(dividendData["scheme_id"])
	folioID := parseString(dividendData["folio_id"])

	// Get scheme name from database
	var schemeName string
	if err := executor.QueryRow(ctx, `SELECT scheme_name FROM investment.masterscheme WHERE scheme_id = $1`, schemeID).Scan(&schemeName); err != nil {
		schemeName = schemeID
	}

	// Fetch bank account - if no folio, find one linked to this scheme
	var bankAccount *BankAccountInfo
	var err error

	if folioID != "" {
		// Folio specified - use it directly
		bankAccount, err = GetBankAccountFromFolio(ctx, executor, folioID)
		if err != nil {
			return nil, fmt.Errorf("failed to get folio bank account for dividend: %w", err)
		}
	} else {
		// No folio specified - use comprehensive JOIN to find bank account via scheme
		query := `
			SELECT 
				COALESCE(ba.account_number, '') AS account_number,
				COALESCE(ba.account_nickname, '') AS account_name,
				COALESCE(b.bank_name, '') AS bank_name,
				COALESCE(e.entity_name, ec.entity_name, '') AS entity_name,
				f.folio_id::text
			FROM investment.folioschememapping fsm
			JOIN investment.masterfolio f ON f.folio_id = fsm.folio_id
			LEFT JOIN public.masterbankaccount ba ON (
				ba.account_number = f.default_redemption_account 
				OR ba.account_id = f.default_redemption_account
				OR ba.account_nickname = f.default_redemption_account
			)
			LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
			LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
			LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
			WHERE fsm.scheme_id = $1
			  AND COALESCE(fsm.status, 'Active') = 'Active'
			  AND COALESCE(f.is_deleted, false) = false
			  AND ba.account_number IS NOT NULL
			ORDER BY f.created_at DESC
			LIMIT 1
		`
		
		var linkedFolioID string
		info := &BankAccountInfo{}
		err := executor.QueryRow(ctx, query, schemeID).Scan(
			&info.AccountNumber,
			&info.AccountName,
			&info.BankName,
			&info.EntityName,
			&linkedFolioID,
		)
		
		if err != nil {
			return nil, fmt.Errorf("no bank account found for scheme %s - ensure folio-scheme mapping and bank account are configured: %w", schemeID, err)
		}
		
		if info.AccountNumber == "" {
			return nil, fmt.Errorf("no bank account configured for any folio linked to scheme %s", schemeID)
		}
		
		folioID = linkedFolioID
		bankAccount = info
	}

	je := &JournalEntry{
		ActivityID: activityID,
		EntityID:   bankAccount.EntityName,
		EntityName: bankAccount.EntityName,
		FolioID:    folioID,
		EntryDate:  time.Now(),
		Lines:      []JournalEntryLine{},
	}

	roundedAmount := RoundAmount(dividendAmount, settings.CurrencyPrecision)

	if transactionType == "PAYOUT" {
		// Dividend Payout: DR Bank Account, CR Dividend Income
		je.EntryType = "DIVIDEND_PAYOUT"
		je.Description = fmt.Sprintf("Dividend payout from %s", schemeName)

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    1,
			AccountNumber: bankAccount.AccountNumber,
			AccountName:   bankAccount.AccountName + " - " + bankAccount.BankName,
			AccountType:   "ASSET",
			DebitAmount:   roundedAmount,
			CreditAmount:  0,
			SchemeID:      schemeID,
			FolioID:       folioID,
			Narration:     fmt.Sprintf("Dividend payout - %s", schemeName),
		})

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    2,
			AccountNumber: "4201", // Dividend Income (standard GL code)
			AccountName:   "Dividend Income",
			AccountType:   "INCOME",
			DebitAmount:   0,
			CreditAmount:  roundedAmount,
			SchemeID:      schemeID,
			FolioID:       folioID,
			Narration:     fmt.Sprintf("Dividend income - %s", schemeName),
		})

	} else if transactionType == "REINVEST" {
		// Dividend Reinvestment: DR Bank Account (Investment), CR Dividend Income
		je.EntryType = "DIVIDEND_REINVEST"
		je.Description = fmt.Sprintf("Dividend reinvestment in %s", schemeName)

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    1,
			AccountNumber: bankAccount.AccountNumber,
			AccountName:   bankAccount.AccountName + " - " + bankAccount.BankName,
			AccountType:   "ASSET",
			DebitAmount:   roundedAmount,
			CreditAmount:  0,
			SchemeID:      schemeID,
			FolioID:       folioID,
			Narration:     fmt.Sprintf("Dividend reinvested - %s", schemeName),
		})

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    2,
			AccountNumber: "4201", // Dividend Income
			AccountName:   "Dividend Income",
			AccountType:   "INCOME",
			DebitAmount:   0,
			CreditAmount:  roundedAmount,
			SchemeID:      schemeID,
			FolioID:       folioID,
			Narration:     fmt.Sprintf("Dividend income - %s", schemeName),
		})
	}

	je.TotalDebit = roundedAmount
	je.TotalCredit = roundedAmount

	return je, nil
}

// GenerateJournalEntryForFVO creates journal entry for Fair Value Override
func GenerateJournalEntryForFVO(ctx context.Context, executor DBExecutor, settings *SettingsCache, activityID string, fvoData map[string]interface{}) (*JournalEntry, error) {
	log.Printf("[DEBUG FVO GEN] GenerateJournalEntryForFVO called (executor type: %T)", executor)
	variance := parseFloat(fvoData["variance"])
	schemeID := parseString(fvoData["scheme_id"])
	
	// Get scheme name from database
	log.Printf("[DEBUG FVO GEN] About to query masterscheme for scheme_id: %s", schemeID)
	var schemeName string
	row := executor.QueryRow(ctx, `SELECT scheme_name FROM investment.masterscheme WHERE scheme_id = $1`, schemeID)
	if err := row.Scan(&schemeName); err != nil {
		log.Printf("[DEBUG FVO GEN] Scheme query failed: %v, using scheme_id as name", err)
		schemeName = schemeID
	}
	log.Printf("[DEBUG FVO GEN] Scheme name resolved: %s", schemeName)

	je := &JournalEntry{
		ActivityID: activityID,
		EntryDate:  time.Now(),
		Lines:      []JournalEntryLine{},
	}

	roundedAmount := RoundAmount(math.Abs(variance), settings.CurrencyPrecision)

	if variance > 0 {
		// FVO Upward: DR Investment Account, CR FVO Gain (Equity - 3101)
		je.EntryType = "FVO_UPWARD"
		je.Description = fmt.Sprintf("Fair value adjustment (upward) on %s", schemeName)

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    1,
			AccountNumber: "1200", // Investment Account
			AccountName:   "Mutual Fund Investments",
			AccountType:   "ASSET",
			DebitAmount:   roundedAmount,
			CreditAmount:  0,
			SchemeID:      schemeID,
			Narration:     fmt.Sprintf("FVO upward adjustment - %s", schemeName),
		})

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    2,
			AccountNumber: "3101", // FVO Gain (Equity)
			AccountName:   "Fair Value Adjustment Gain",
			AccountType:   "EQUITY",
			DebitAmount:   0,
			CreditAmount:  roundedAmount,
			SchemeID:      schemeID,
			Narration:     fmt.Sprintf("FVO gain - %s", schemeName),
		})

	} else if variance < 0 {
		// FVO Downward: DR FVO Loss (Equity - 3102), CR Investment Account
		je.EntryType = "FVO_DOWNWARD"
		je.Description = fmt.Sprintf("Fair value adjustment (downward) on %s", schemeName)

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    1,
			AccountNumber: "3102", // FVO Loss (Equity)
			AccountName:   "Fair Value Adjustment Loss",
			AccountType:   "EQUITY",
			DebitAmount:   roundedAmount,
			CreditAmount:  0,
			SchemeID:      schemeID,
			Narration:     fmt.Sprintf("FVO downward adjustment - %s", schemeName),
		})

		je.Lines = append(je.Lines, JournalEntryLine{
			LineNumber:    2,
			AccountNumber: "1200", // Investment Account
			AccountName:   "Mutual Fund Investments",
			AccountType:   "ASSET",
			DebitAmount:   0,
			CreditAmount:  roundedAmount,
			SchemeID:      schemeID,
			Narration:     fmt.Sprintf("FVO loss - %s", schemeName),
		})
	}

	je.TotalDebit = roundedAmount
	je.TotalCredit = roundedAmount

	return je, nil
}

// GenerateJournalEntryForCorporateAction creates journal entry for corporate actions
func GenerateJournalEntryForCorporateAction(ctx context.Context, executor DBExecutor, settings *SettingsCache, activityID string, corpActionData map[string]interface{}) (*JournalEntry, error) {
	// Corporate actions are handled by the processor which updates portfolio_snapshot
	// No journal entries needed as these are scheme-level operations
	return nil, nil
}

// SaveJournalEntry persists a journal entry to the database
func SaveJournalEntry(ctx context.Context, tx DBExecutor, je *JournalEntry) error {
	log.Printf("[DEBUG SAVE] SaveJournalEntry called for activity_id: %s, entry_type: %s", je.ActivityID, je.EntryType)

	// Validate balanced entry
	if je.TotalDebit != je.TotalCredit {
		return fmt.Errorf("journal entry not balanced: debit=%.2f, credit=%.2f", je.TotalDebit, je.TotalCredit)
	}
	log.Printf("[DEBUG SAVE] Journal entry validated (balanced)")

	// Insert master journal entry
	entryQuery := `
		INSERT INTO investment.accounting_journal_entry 
		(activity_id, entity_id, entity_name, folio_id, demat_id, entry_date, accounting_period, entry_type, description, total_debit, total_credit, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'POSTED')
		RETURNING entry_id
	`

	log.Printf("[DEBUG SAVE] About to execute INSERT query using tx (type: %T)", tx)
	var entryID string
	err := tx.QueryRow(ctx, entryQuery,
		je.ActivityID,
		je.EntityID,
		je.EntityName,
		je.FolioID,
		je.DematID,
		je.EntryDate,
		je.AccountingPeriod,
		je.EntryType,
		je.Description,
		je.TotalDebit,
		je.TotalCredit,
	).Scan(&entryID)

	if err != nil {
		log.Printf("[DEBUG SAVE] INSERT failed: %v", err)
		return fmt.Errorf("failed to insert journal entry: %w", err)
	}
	log.Printf("[DEBUG SAVE] INSERT successful, entry_id: %s", entryID)

	// Insert line items
	lineQuery := `
		INSERT INTO investment.accounting_journal_entry_line 
		(entry_id, line_number, account_number, account_name, account_type, debit_amount, credit_amount, scheme_id, folio_id, demat_id, narration)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`

	for _, line := range je.Lines {
		_, err := tx.Exec(ctx, lineQuery,
			entryID,
			line.LineNumber,
			line.AccountNumber,
			line.AccountName,
			line.AccountType,
			line.DebitAmount,
			line.CreditAmount,
			line.SchemeID,
			line.FolioID,
			line.DematID,
			line.Narration,
		)

		if err != nil {
			return fmt.Errorf("failed to insert journal entry line: %w", err)
		}
	}

	log.Printf("Journal entry created: entry_id=%s, type=%s, entity=%s, amount=%.2f",
		entryID, je.EntryType, je.EntityID, je.TotalDebit)

	return nil
}

// Helper functions
func parseString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func parseFloat(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return 0
}

func parseTime(v interface{}) time.Time {
	if v == nil {
		return time.Now()
	}
	if t, ok := v.(time.Time); ok {
		return t
	}
	if s, ok := v.(string); ok {
		if t, err := time.Parse("2006-01-02", s); err == nil {
			return t
		}
	}
	return time.Now()
}
