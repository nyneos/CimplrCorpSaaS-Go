package rules

type filterReq struct {
	Field       string `json:"field"`
	FieldType   string `json:"field_type"`
	Op          string `json:"op"`
	Value       string `json:"value"`
	Value2      string `json:"value2"`
	Conjunction string `json:"conjunction"`
}

type attachmentReq struct {
	DocumentTemplateID string `json:"document_template_id"`
	OutputFormat       string `json:"output_format"`
}

type destinationReq struct {
	DestinationID    string `json:"destination_id,omitempty"`
	ClientRef        string `json:"client_ref,omitempty"`
	DestinationType  string `json:"destination_type"`
	IsEnabled        bool   `json:"is_enabled"`
	TargetURI        string `json:"target_uri,omitempty"`
	TargetLabel      string `json:"target_label,omitempty"`
	OutputNamePrefix string `json:"output_name_prefix,omitempty"`
	AppendDatetime   *bool  `json:"append_datetime,omitempty"` // nil → true
	PackageMode      string `json:"package_mode,omitempty"`    // FILES | ZIP
	// SFTP (same shape as email_svc.transformation_rule_destinations)
	SftpHost     string `json:"sftp_host,omitempty"`
	SftpPort     int    `json:"sftp_port,omitempty"`
	SftpUser     string `json:"sftp_user,omitempty"`
	SftpPassword string `json:"sftp_password,omitempty"`
	SftpFolder   string `json:"sftp_folder,omitempty"`
	// WEBHOOK
	APIURL       string `json:"api_url,omitempty"`
	APIAuthToken string `json:"api_auth_token,omitempty"`
}

// emailRecipientReq is one To/Cc address on a generation rule version.
type emailRecipientReq struct {
	DestinationID  string `json:"destination_id,omitempty"`
	DestinationRef string `json:"destination_ref,omitempty"`
	AddressRole    string `json:"address_role"` // TO | CC
	Email          string `json:"email"`
}

type triggerReq struct {
	TriggerID     string `json:"trigger_id,omitempty"`
	TriggerType   string `json:"trigger_type"`
	EventCode     string `json:"event_code,omitempty"`
	SourceIDField string `json:"source_id_field,omitempty"`
	DateField     string `json:"date_field,omitempty"`
	OffsetDays    *int   `json:"offset_days,omitempty"`
	IsEnabled     bool   `json:"is_enabled"`
}

// bankAccountScopeReq is one bank+account pair for FetchSourceData.
type bankAccountScopeReq struct {
	BankID        string `json:"bank_id"`
	AccountNumber string `json:"account_number"`
}

// ruleAuditRow is the minimal projection of a pending dms_svc.generation_rule_audit
// row needed to action a checker decision (approve/reject).
type ruleAuditRow struct {
	AuditID          string
	ActionType       string
	VersionID        *string
	NewName          *string
	NewModuleCode    *string
	NewSubModuleCode *string
	NewStatus        *string
}

type versionSummary struct {
	VersionID        string  `json:"version_id"`
	VersionNo        int     `json:"version_no"`
	Status           string  `json:"status"`
	TimeWindowType   string  `json:"time_window_type"`
	TimeWindowValue  *int    `json:"time_window_value"`
	TimeWindowUnit   *string `json:"time_window_unit"`
	CustomStart      *string `json:"custom_start"`
	CustomEnd        *string `json:"custom_end"`
	ScheduleType     string  `json:"schedule_type"`
	CronExpr         *string `json:"cron_expr"`
	RepeatKind       string  `json:"repeat_kind"`
	ScheduleTime     *string `json:"schedule_time"`
	ScheduleWeekday  *int    `json:"schedule_weekday"`
	ScheduleMonthDay *int    `json:"schedule_month_day"`
	ScheduleTimezone string  `json:"schedule_timezone"`
	RowExpandMode    string  `json:"row_expand_mode"`
	DataRowFrom      int     `json:"data_row_from"`
	DataRowTo        int     `json:"data_row_to"`
	CreatedBy        string  `json:"created_by"`
	CreatedAt        string  `json:"created_at"`
	ApprovedBy       *string `json:"approved_by"`
	ApprovedAt       *string `json:"approved_at"`
	IsCurrent        bool    `json:"is_current"`
}

type ruleListItem struct {
	RuleID           string  `json:"rule_id"`
	Name             string  `json:"name"`
	Description      string  `json:"description"`
	ModuleCode       string  `json:"module_code"`
	SubModuleCode    string  `json:"sub_module_code"`
	EntityID         *string `json:"entity_id"`
	EntityName       *string `json:"entity_name"`
	Status           string  `json:"status"`
	ProcessingStatus string  `json:"processing_status"`
	CurrentVersionID *string `json:"current_version_id"`
	CreatedBy        string  `json:"created_by"`
	CreatedAt        string  `json:"created_at"`
	LastModifiedBy   string  `json:"last_modified_by"`
	LastModifiedAt   string  `json:"last_modified_at"`
}

type ruleDetail struct {
	ruleListItem
	Versions                []versionSummary      `json:"versions"`
	TimeWindowType          string                `json:"time_window_type,omitempty"`
	TimeWindowValue         *int                  `json:"time_window_value,omitempty"`
	TimeWindowUnit          *string               `json:"time_window_unit,omitempty"`
	CustomStart             *string               `json:"custom_start,omitempty"`
	CustomEnd               *string               `json:"custom_end,omitempty"`
	ScheduleType            string                `json:"schedule_type,omitempty"`
	CronExpr                *string               `json:"cron_expr,omitempty"`
	RepeatKind              string                `json:"repeat_kind,omitempty"`
	ScheduleTime            *string               `json:"schedule_time,omitempty"`
	ScheduleWeekday         *int                  `json:"schedule_weekday,omitempty"`
	ScheduleMonthDay        *int                  `json:"schedule_month_day,omitempty"`
	ScheduleTimezone        string                `json:"schedule_timezone,omitempty"`
	RowExpandMode           string                `json:"row_expand_mode,omitempty"`
	DataRowFrom             int                   `json:"data_row_from,omitempty"`
	DataRowTo               int                   `json:"data_row_to,omitempty"`
	Filters                 []filterReq           `json:"filters,omitempty"`
	Attachments             []attachmentReq       `json:"attachments,omitempty"`
	Destinations            []destinationReq      `json:"destinations,omitempty"`
	EmailRecipients         []emailRecipientReq   `json:"email_recipients,omitempty"`
	BankAccountScope        []bankAccountScopeReq `json:"bank_account_scope,omitempty"`
	NotificationTemplateIDs []string              `json:"notification_template_ids,omitempty"`
	Triggers                []triggerReq          `json:"triggers,omitempty"`
}
