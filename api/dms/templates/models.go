package templates

import "encoding/json"

// chartFilterReq mirrors the dashboard-builder widget filter shape
// (field/type/op/value/value2/conjunction) so a template chart placeholder's
// filters never disagree with what a live dashboard widget means by a filter.
type chartFilterReq struct {
	Field       string `json:"field"`
	Op          string `json:"op"`
	Value       string `json:"value"`
	Value2      string `json:"value2"`
	Conjunction string `json:"conjunction"`
}

type chartPlaceholderReq struct {
	PlaceholderKey string           `json:"placeholder_key"`
	ChartType      string           `json:"chart_type"`
	DataSource     string           `json:"data_source"`
	DimensionField string           `json:"dimension_field"`
	MeasureField   string           `json:"measure_field"`
	Filters        []chartFilterReq `json:"filters"`
}

type mergeFieldReq struct {
	FieldKey             string `json:"field_key"`
	DomainCatalogFieldID string `json:"domain_catalog_field_id"`
	DisplayFormat        string `json:"display_format"`
}

// templateAuditRow is the minimal projection of a pending dms_svc.template_audit
// row needed to action a checker decision (approve/reject) — see findPendingAudit.
type templateAuditRow struct {
	AuditID          string
	ActionType       string
	VersionID        *string
	NewName          *string
	NewModuleCode    *string
	NewSubModuleCode *string
	NewStatus        *string
}

// versionSummary is the list-view projection of a template_version row.
type versionSummary struct {
	VersionID    string  `json:"version_id"`
	VersionNo    int     `json:"version_no"`
	Status       string  `json:"status"`
	Source       string  `json:"source"`
	CreatedBy    string  `json:"created_by"`
	CreatedAt    string  `json:"created_at"`
	ApprovedBy   *string `json:"approved_by"`
	ApprovedAt   *string `json:"approved_at"`
	SourceFile   *string `json:"source_file_name"`
	IsCurrent    bool    `json:"is_current"`
}

// templateListItem is the list-view projection of a template row.
type templateListItem struct {
	TemplateID        string  `json:"template_id"`
	Name               string  `json:"name"`
	Description        string  `json:"description"`
	TemplateType        string  `json:"template_type"`
	Kind                 string  `json:"kind"`
	ModuleCode           string  `json:"module_code"`
	SubModuleCode        string  `json:"sub_module_code"`
	EntityID             *string `json:"entity_id"`
	EntityName           *string `json:"entity_name"`
	Status                string  `json:"status"`
	ProcessingStatus       string  `json:"processing_status"`
	CurrentVersionID        *string `json:"current_version_id"`
	CreatedBy                string  `json:"created_by"`
	CreatedAt                 string  `json:"created_at"`
	LastModifiedBy              string  `json:"last_modified_by"`
	LastModifiedAt               string  `json:"last_modified_at"`
}

// templateDetail is the full detail-view payload for the template studio.
type templateDetail struct {
	templateListItem
	Versions         []versionSummary       `json:"versions"`
	ContentJSON      json.RawMessage        `json:"content_json,omitempty"`
	MergeFields      []mergeFieldReq        `json:"merge_fields,omitempty"`
	ChartPlaceholders []chartPlaceholderReq `json:"chart_placeholders,omitempty"`
}
