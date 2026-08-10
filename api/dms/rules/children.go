package rules

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

var allowedDestinationTypes = map[string]struct{}{
	"S3_ARCHIVE": {},
	"EMAIL":      {},
	"IN_APP":     {},
	"SFTP":       {},
	"WEBHOOK":    {},
	"SHAREPOINT": {},
	"LOCAL":      {},
}

// insertVersionChildrenParams groups the child-record bundle for a rule
// version insert, keeping insertVersionChildren's parameter count within
// SonarQube's limit.
type insertVersionChildrenParams struct {
	RuleID                  string
	VersionID               string
	Actor                   string
	ActionType              string
	Filters                 []filterReq
	Attachments             []attachmentReq
	Destinations            []destinationReq
	EmailRecipients         []emailRecipientReq
	BankAccountScope        []bankAccountScopeReq
	NotificationTemplateIDs []string
	Triggers                []triggerReq
}

// insertVersionChildren attaches filters, document attachments, destinations,
// email recipients, bank/account scope, and notification-template links.
// Also appends destination + email-recipient audit rows (old_/new_ pairs).
func insertVersionChildren(ctx context.Context, tx pgx.Tx, p insertVersionChildrenParams) error {
	ruleID := p.RuleID
	versionID := p.VersionID
	actor := p.Actor
	actionType := p.ActionType
	filters := p.Filters
	attachments := p.Attachments
	destinations := p.Destinations
	emailRecipients := p.EmailRecipients
	bankAccountScope := p.BankAccountScope
	notificationTemplateIDs := p.NotificationTemplateIDs
	triggers := p.Triggers

	if actionType == "" {
		actionType = "CREATE"
	}
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
	destinationIDsByRef := make(map[string]string)
	firstEmailDestinationID := ""
	for i, d := range destinations {
		typ := strings.TrimSpace(strings.ToUpper(d.DestinationType))
		if typ == "" {
			continue
		}
		if _, ok := allowedDestinationTypes[typ]; !ok {
			return fmt.Errorf("destination type %q is not allowed", typ)
		}
		appendDT := true
		if d.AppendDatetime != nil {
			appendDT = *d.AppendDatetime
		}
		packageMode := strings.TrimSpace(strings.ToUpper(d.PackageMode))
		if packageMode == "" {
			packageMode = "FILES"
		}
		if packageMode != "FILES" && packageMode != "ZIP" {
			return fmt.Errorf("destination package_mode must be FILES or ZIP")
		}
		sftpPort := d.SftpPort
		if sftpPort <= 0 {
			sftpPort = 22
		}
		if typ == "SFTP" && d.IsEnabled {
			if strings.TrimSpace(d.SftpHost) == "" || strings.TrimSpace(d.SftpUser) == "" {
				return fmt.Errorf("sftp_host and sftp_user are required for SFTP destination")
			}
		}
		if typ == "WEBHOOK" && d.IsEnabled && strings.TrimSpace(d.APIURL) == "" {
			return fmt.Errorf("api_url is required for WEBHOOK destination")
		}

		var destID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO dms_svc.generation_rule_destination
				(version_id, destination_type, is_enabled, sort_order, target_uri, target_label,
				 output_name_prefix, append_datetime, package_mode,
				 sftp_host, sftp_port, sftp_user, sftp_password, sftp_folder,
				 api_url, api_auth_token)
			VALUES ($1::uuid, $2, $3, $4, NULLIF($5,''), COALESCE($6,''), COALESCE($7,''), $8,
			        $9, COALESCE($10,''), $11, COALESCE($12,''), COALESCE($13,''), COALESCE($14,''),
			        COALESCE($15,''), COALESCE($16,''))
			RETURNING destination_id::text`,
			versionID, typ, d.IsEnabled, i, d.TargetURI, d.TargetLabel,
			strings.TrimSpace(d.OutputNamePrefix), appendDT, packageMode,
			strings.TrimSpace(d.SftpHost), sftpPort, strings.TrimSpace(d.SftpUser),
			d.SftpPassword, strings.TrimSpace(d.SftpFolder),
			strings.TrimSpace(d.APIURL), d.APIAuthToken,
		).Scan(&destID); err != nil {
			return fmt.Errorf("destination %q: %w", typ, err)
		}
		ref := strings.TrimSpace(d.ClientRef)
		if ref == "" {
			ref = fmt.Sprintf("destination-%d", i)
		}
		if _, exists := destinationIDsByRef[ref]; exists {
			return fmt.Errorf("destination client_ref %q is duplicated", ref)
		}
		destinationIDsByRef[ref] = destID
		if typ == "EMAIL" && firstEmailDestinationID == "" {
			firstEmailDestinationID = destID
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO dms_svc.generation_rule_destination_audit (
				rule_id, version_id, destination_id, action_type, actor_id,
				new_destination_type, new_is_enabled, new_sort_order,
				new_target_uri, new_target_label, new_output_name_prefix, new_append_datetime,
				new_package_mode,
				new_sftp_host, new_sftp_port, new_sftp_user, new_sftp_folder,
				new_api_url, new_is_deleted
			) VALUES (
				$1::uuid, $2::uuid, $3::uuid, $4, $5,
				$6, $7, $8,
				NULLIF($9,''), NULLIF($10,''), NULLIF($11,''), $12, $13,
				NULLIF($14,''), $15, NULLIF($16,''), NULLIF($17,''),
				NULLIF($18,''), false
			)`,
			ruleID, versionID, destID, actionType, actor,
			typ, d.IsEnabled, i,
			d.TargetURI, d.TargetLabel, strings.TrimSpace(d.OutputNamePrefix), appendDT, packageMode,
			strings.TrimSpace(d.SftpHost), sftpPort, strings.TrimSpace(d.SftpUser), strings.TrimSpace(d.SftpFolder),
			strings.TrimSpace(d.APIURL),
		); err != nil {
			return fmt.Errorf("destination audit %q: %w", typ, err)
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
		destinationID := strings.TrimSpace(er.DestinationID)
		if destinationID == "" {
			destinationID = destinationIDsByRef[strings.TrimSpace(er.DestinationRef)]
		}
		if destinationID == "" {
			destinationID = firstEmailDestinationID // backwards-compatible flat recipients
		}
		if destinationID == "" {
			return fmt.Errorf("email recipient %q has no EMAIL destination", email)
		}
		key := destinationID + "|" + role + "|" + email
		if _, dup := seenEmail[key]; dup {
			continue
		}
		seenEmail[key] = struct{}{}
		var recipientID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO dms_svc.generation_rule_email_recipient
				(version_id, destination_id, address_role, email, sort_order)
			SELECT $1::uuid, d.destination_id, $3, $4, $5
			FROM dms_svc.generation_rule_destination d
			WHERE d.destination_id = $2::uuid
			  AND d.version_id = $1::uuid
			  AND d.destination_type = 'EMAIL'
			  AND d.is_deleted = false
			RETURNING recipient_id::text`,
			versionID, destinationID, role, email, i,
		).Scan(&recipientID); err != nil {
			return fmt.Errorf("email recipient %s %q: %w", role, email, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dms_svc.generation_rule_email_recipient_audit (
				rule_id, version_id, destination_id, recipient_id, action_type, actor_id,
				new_destination_id, new_address_role, new_email, new_sort_order
			) VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5, $6, $3::uuid, $7, $8, $9)`,
			ruleID, versionID, destinationID, recipientID, actionType, actor, role, email, i,
		); err != nil {
			return fmt.Errorf("email recipient audit %s %q: %w", role, email, err)
		}
	}

	allowedTriggers := map[string]struct{}{
		"MANUAL": {}, "SCHEDULE": {}, "SCHEDULED": {}, "DATE_RELATIVE": {},
		"ON_CREATE": {}, "ON_APPROVE": {}, "ON_EDIT": {}, "ON_FIELD_CHANGE": {}, "ON_DELETE": {},
		"POST_CREATE": {}, "POST_APPROVE": {}, "POST_UPLOAD": {},
		"POST_EDIT": {}, "POST_DELETE": {}, "POST_REJECT": {},
	}
	for i, trigger := range triggers {
		triggerType := strings.ToUpper(strings.TrimSpace(trigger.TriggerType))
		if _, ok := allowedTriggers[triggerType]; !ok {
			return fmt.Errorf("trigger type %q is not allowed", triggerType)
		}
		if triggerType == "DATE_RELATIVE" && strings.TrimSpace(trigger.DateField) == "" {
			return fmt.Errorf("date_field is required for DATE_RELATIVE trigger")
		}
		var triggerID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO dms_svc.generation_rule_trigger (
				version_id, trigger_type, event_code, source_id_field,
				date_field, offset_days, is_enabled, sort_order
			) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8)
			RETURNING trigger_id::text`,
			versionID, triggerType, strings.TrimSpace(trigger.EventCode),
			strings.TrimSpace(trigger.SourceIDField), strings.TrimSpace(trigger.DateField),
			trigger.OffsetDays, trigger.IsEnabled, i,
		).Scan(&triggerID); err != nil {
			return fmt.Errorf("trigger %q: %w", triggerType, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dms_svc.generation_rule_trigger_audit (
				rule_id, version_id, trigger_id, action_type, actor_id,
				new_trigger_type, new_event_code, new_source_id_field,
				new_date_field, new_offset_days, new_is_enabled, new_sort_order
			) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			ruleID, versionID, triggerID, actionType, actor,
			triggerType, strings.TrimSpace(trigger.EventCode),
			strings.TrimSpace(trigger.SourceIDField), strings.TrimSpace(trigger.DateField),
			trigger.OffsetDays, trigger.IsEnabled, i,
		); err != nil {
			return fmt.Errorf("trigger audit %q: %w", triggerType, err)
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
	Triggers                []triggerReq
}

func loadVersionChildren(ctx context.Context, tx pgx.Tx, versionID string) (versionChildren, error) {
	out := versionChildren{
		Filters:                 make([]filterReq, 0),
		Attachments:             make([]attachmentReq, 0),
		Destinations:            make([]destinationReq, 0),
		EmailRecipients:         make([]emailRecipientReq, 0),
		BankAccountScope:        make([]bankAccountScopeReq, 0),
		NotificationTemplateIDs: make([]string, 0),
		Triggers:                make([]triggerReq, 0),
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
		SELECT destination_id::text, destination_type, is_enabled, COALESCE(target_uri,''), COALESCE(target_label,''),
		       COALESCE(output_name_prefix,''), append_datetime, COALESCE(package_mode,'FILES'),
		       COALESCE(sftp_host,''), COALESCE(sftp_port, 22), COALESCE(sftp_user,''),
		       COALESCE(sftp_password,''), COALESCE(sftp_folder,''),
		       COALESCE(api_url,''), COALESCE(api_auth_token,'')
		FROM dms_svc.generation_rule_destination
		WHERE version_id = $1::uuid AND is_deleted = false
		ORDER BY sort_order`, versionID)
	if err != nil {
		return out, err
	}
	for dRows.Next() {
		var d destinationReq
		var appendDT bool
		if err := dRows.Scan(&d.DestinationID, &d.DestinationType, &d.IsEnabled, &d.TargetURI, &d.TargetLabel,
			&d.OutputNamePrefix, &appendDT, &d.PackageMode,
			&d.SftpHost, &d.SftpPort, &d.SftpUser, &d.SftpPassword, &d.SftpFolder,
			&d.APIURL, &d.APIAuthToken); err != nil {
			dRows.Close()
			return out, err
		}
		d.AppendDatetime = &appendDT
		// Never echo secrets on detail reads — UI can re-enter on edit.
		d.SftpPassword = ""
		d.APIAuthToken = ""
		out.Destinations = append(out.Destinations, d)
	}
	dRows.Close()

	eRows, err := tx.Query(ctx, `
		SELECT COALESCE(destination_id::text,''), address_role, email
		FROM dms_svc.generation_rule_email_recipient
		WHERE version_id = $1::uuid
		ORDER BY address_role, sort_order, email`, versionID)
	if err != nil {
		return out, err
	}
	for eRows.Next() {
		var er emailRecipientReq
		if err := eRows.Scan(&er.DestinationID, &er.AddressRole, &er.Email); err != nil {
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

	tRows, err := tx.Query(ctx, `
		SELECT trigger_id::text, trigger_type, event_code, source_id_field,
		       date_field, offset_days, is_enabled
		FROM dms_svc.generation_rule_trigger
		WHERE version_id = $1::uuid
		ORDER BY sort_order`, versionID)
	if err != nil {
		return out, err
	}
	for tRows.Next() {
		var trigger triggerReq
		if err := tRows.Scan(
			&trigger.TriggerID, &trigger.TriggerType, &trigger.EventCode,
			&trigger.SourceIDField, &trigger.DateField, &trigger.OffsetDays,
			&trigger.IsEnabled,
		); err != nil {
			tRows.Close()
			return out, err
		}
		out.Triggers = append(out.Triggers, trigger)
	}
	tRows.Close()

	return out, nil
}
