package rules

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// insertVersionChildren attaches filters, document attachments, destinations,
// email recipients, bank/account scope, and notification-template links.
func insertVersionChildren(
	ctx context.Context,
	tx pgx.Tx,
	versionID, actor string,
	filters []filterReq,
	attachments []attachmentReq,
	destinations []destinationReq,
	emailRecipients []emailRecipientReq,
	bankAccountScope []bankAccountScopeReq,
	notificationTemplateIDs []string,
) error {
	for i, f := range filters {
		field := strings.TrimSpace(f.Field)
		if field == "" {
			continue
		}
		conjunction := strings.TrimSpace(strings.ToUpper(f.Conjunction))
		if conjunction == "" {
			conjunction = "AND"
		}
		fieldType := strings.TrimSpace(strings.ToLower(f.FieldType))
		if fieldType == "" {
			fieldType = "text"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dms_svc.generation_rule_filter (version_id, field, field_type, op, value, value2, conjunction, sort_order)
			VALUES ($1::uuid, $2, $3, $4, NULLIF($5,''), NULLIF($6,''), $7, $8)`,
			versionID, field, fieldType, f.Op, f.Value, f.Value2, conjunction, i); err != nil {
			return fmt.Errorf("filter %q: %w", field, err)
		}
	}

	for i, a := range attachments {
		docTemplateID := strings.TrimSpace(a.DocumentTemplateID)
		if docTemplateID == "" {
			continue
		}
		format := strings.TrimSpace(strings.ToUpper(a.OutputFormat))
		if format == "" {
			format = "PDF"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dms_svc.generation_rule_attachment (version_id, document_template_id, output_format, sort_order)
			VALUES ($1::uuid, $2::uuid, $3, $4)`,
			versionID, docTemplateID, format, i); err != nil {
			return fmt.Errorf("attachment %q: %w", docTemplateID, err)
		}
	}

	if len(destinations) == 0 {
		destinations = []destinationReq{
			{DestinationType: "S3_ARCHIVE", IsEnabled: true},
			{DestinationType: "EMAIL", IsEnabled: true},
		}
	}
	seenDest := make(map[string]struct{})
	for i, d := range destinations {
		typ := strings.TrimSpace(strings.ToUpper(d.DestinationType))
		if typ == "" {
			continue
		}
		if _, dup := seenDest[typ]; dup {
			continue
		}
		seenDest[typ] = struct{}{}
		appendDT := true
		if d.AppendDatetime != nil {
			appendDT = *d.AppendDatetime
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dms_svc.generation_rule_destination
				(version_id, destination_type, is_enabled, sort_order, target_uri, target_label,
				 output_name_prefix, append_datetime)
			VALUES ($1::uuid, $2, $3, $4, NULLIF($5,''), COALESCE($6,''), COALESCE($7,''), $8)`,
			versionID, typ, d.IsEnabled, i, d.TargetURI, d.TargetLabel,
			strings.TrimSpace(d.OutputNamePrefix), appendDT); err != nil {
			return fmt.Errorf("destination %q: %w", typ, err)
		}
	}

	seenEmail := make(map[string]struct{})
	for i, er := range emailRecipients {
		role := strings.TrimSpace(strings.ToUpper(er.AddressRole))
		email := strings.ToLower(strings.TrimSpace(er.Email))
		if role != "TO" && role != "CC" {
			continue
		}
		if email == "" || !strings.Contains(email, "@") {
			continue
		}
		key := role + "|" + email
		if _, dup := seenEmail[key]; dup {
			continue
		}
		seenEmail[key] = struct{}{}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dms_svc.generation_rule_email_recipient (version_id, address_role, email, sort_order)
			VALUES ($1::uuid, $2, $3, $4)`,
			versionID, role, email, i); err != nil {
			return fmt.Errorf("email recipient %s %q: %w", role, email, err)
		}
	}

	seenScope := make(map[string]struct{})
	for i, s := range bankAccountScope {
		acct := strings.TrimSpace(s.AccountNumber)
		if acct == "" {
			continue
		}
		bankID := strings.TrimSpace(s.BankID)
		key := strings.ToLower(bankID) + "|" + strings.ToLower(acct)
		if _, dup := seenScope[key]; dup {
			continue
		}
		seenScope[key] = struct{}{}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dms_svc.generation_rule_bank_account_scope
				(version_id, bank_id, account_number, sort_order)
			VALUES ($1::uuid, COALESCE($2,''), $3, $4)`,
			versionID, bankID, acct, i); err != nil {
			return fmt.Errorf("bank account scope %q: %w", acct, err)
		}
	}

	seen := make(map[string]struct{}, len(notificationTemplateIDs))
	for _, id := range notificationTemplateIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dms_svc.generation_rule_notification_template (version_id, template_id, created_by)
			VALUES ($1::uuid, $2, $3)
			ON CONFLICT (version_id, template_id) DO UPDATE SET is_deleted = false`,
			versionID, id, actor); err != nil {
			return fmt.Errorf("notification template %q: %w", id, err)
		}
	}
	return nil
}

// versionChildren is the full child bundle for a rule version.
type versionChildren struct {
	Filters                 []filterReq
	Attachments             []attachmentReq
	Destinations            []destinationReq
	EmailRecipients         []emailRecipientReq
	BankAccountScope        []bankAccountScopeReq
	NotificationTemplateIDs []string
}

func loadVersionChildren(ctx context.Context, tx pgx.Tx, versionID string) (versionChildren, error) {
	out := versionChildren{
		Filters:                 make([]filterReq, 0),
		Attachments:             make([]attachmentReq, 0),
		Destinations:            make([]destinationReq, 0),
		EmailRecipients:         make([]emailRecipientReq, 0),
		BankAccountScope:        make([]bankAccountScopeReq, 0),
		NotificationTemplateIDs: make([]string, 0),
	}

	fRows, err := tx.Query(ctx, `
		SELECT field, field_type, op, COALESCE(value,''), COALESCE(value2,''), conjunction
		FROM dms_svc.generation_rule_filter
		WHERE version_id = $1::uuid ORDER BY sort_order`, versionID)
	if err != nil {
		return out, err
	}
	for fRows.Next() {
		var f filterReq
		if err := fRows.Scan(&f.Field, &f.FieldType, &f.Op, &f.Value, &f.Value2, &f.Conjunction); err != nil {
			fRows.Close()
			return out, err
		}
		out.Filters = append(out.Filters, f)
	}
	fRows.Close()

	aRows, err := tx.Query(ctx, `
		SELECT document_template_id::text, output_format
		FROM dms_svc.generation_rule_attachment
		WHERE version_id = $1::uuid ORDER BY sort_order`, versionID)
	if err != nil {
		return out, err
	}
	for aRows.Next() {
		var a attachmentReq
		if err := aRows.Scan(&a.DocumentTemplateID, &a.OutputFormat); err != nil {
			aRows.Close()
			return out, err
		}
		out.Attachments = append(out.Attachments, a)
	}
	aRows.Close()

	dRows, err := tx.Query(ctx, `
		SELECT destination_type, is_enabled, COALESCE(target_uri,''), COALESCE(target_label,''),
		       COALESCE(output_name_prefix,''), append_datetime
		FROM dms_svc.generation_rule_destination
		WHERE version_id = $1::uuid ORDER BY sort_order`, versionID)
	if err != nil {
		return out, err
	}
	for dRows.Next() {
		var d destinationReq
		var appendDT bool
		if err := dRows.Scan(&d.DestinationType, &d.IsEnabled, &d.TargetURI, &d.TargetLabel,
			&d.OutputNamePrefix, &appendDT); err != nil {
			dRows.Close()
			return out, err
		}
		d.AppendDatetime = &appendDT
		out.Destinations = append(out.Destinations, d)
	}
	dRows.Close()

	eRows, err := tx.Query(ctx, `
		SELECT address_role, email
		FROM dms_svc.generation_rule_email_recipient
		WHERE version_id = $1::uuid
		ORDER BY address_role, sort_order, email`, versionID)
	if err != nil {
		return out, err
	}
	for eRows.Next() {
		var er emailRecipientReq
		if err := eRows.Scan(&er.AddressRole, &er.Email); err != nil {
			eRows.Close()
			return out, err
		}
		out.EmailRecipients = append(out.EmailRecipients, er)
	}
	eRows.Close()

	sRows, err := tx.Query(ctx, `
		SELECT COALESCE(bank_id,''), account_number
		FROM dms_svc.generation_rule_bank_account_scope
		WHERE version_id = $1::uuid
		ORDER BY sort_order, account_number`, versionID)
	if err != nil {
		return out, err
	}
	for sRows.Next() {
		var s bankAccountScopeReq
		if err := sRows.Scan(&s.BankID, &s.AccountNumber); err != nil {
			sRows.Close()
			return out, err
		}
		out.BankAccountScope = append(out.BankAccountScope, s)
	}
	sRows.Close()

	nRows, err := tx.Query(ctx, `
		SELECT template_id FROM dms_svc.generation_rule_notification_template
		WHERE version_id = $1::uuid AND is_deleted = false ORDER BY template_id`, versionID)
	if err != nil {
		return out, err
	}
	for nRows.Next() {
		var id string
		if err := nRows.Scan(&id); err != nil {
			nRows.Close()
			return out, err
		}
		out.NotificationTemplateIDs = append(out.NotificationTemplateIDs, id)
	}
	nRows.Close()

	return out, nil
}
