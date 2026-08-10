package policy

import (
	"encoding/json"
	"fmt"
	"strings"
)

// The shapes below mirror the frontend's RuleConfig union (PolicyEngine/types.ts)
// exactly by JSON key so `config` can be decoded straight off the wire.

type thresholdConfig struct {
	Variable    string  `json:"variable"`
	Operator    string  `json:"operator"`
	Value       float64 `json:"value"`
	// ValueDate (yyyy-mm-dd) is used instead of Value when the rule's CDM
	// variable is date-typed — never both. See database/2026-07-24.sql.
	ValueDate   string `json:"valueDate,omitempty"`
	ValueMode   string `json:"valueMode"`
	PercentBase string `json:"percentBase"`
	Unit        string `json:"unit"`
}

type slabRowConfig struct {
	From        float64  `json:"from"`
	To          *float64 `json:"to"`
	Mode        string   `json:"mode"`
	Action      string   `json:"action"`
	ApprovalRef string   `json:"approvalRef"`
	Label       string   `json:"label"`
}

type slabsConfig struct {
	Variable    string          `json:"variable"`
	Rows        []slabRowConfig `json:"rows"`
	Unit        string          `json:"unit"`
	PercentBase string          `json:"percentBase"`
}

type compositionBucketConfig struct {
	Label    string   `json:"label"`
	Variable string   `json:"variable"`
	Min      *float64 `json:"min"`
	Max      *float64 `json:"max"`
}

type totalCheckConfig struct {
	Variable string   `json:"variable"`
	Min      *float64 `json:"min"`
	Max      *float64 `json:"max"`
}

type compositionConfig struct {
	Buckets    []compositionBucketConfig `json:"buckets"`
	TotalCheck *totalCheckConfig         `json:"totalCheck"`
	Base       string                    `json:"base"`
}

type listConfig struct {
	TargetField   string   `json:"targetField"`
	Mode          string   `json:"mode"`
	ListSource    string   `json:"listSource"`
	Values        []string `json:"values"`
	DynamicRef    string   `json:"dynamicRef"`
	CaseSensitive bool     `json:"caseSensitive"`
}

type formulaConfig struct {
	Expression string  `json:"expression"`
	ReturnType string  `json:"returnType"`
	Operator   string  `json:"operator"`
	Value      float64 `json:"value"`
}

// ruleFields is the flattened representation of a decoded `config` — the
// header columns that live on policy_master plus the 1:many child rows
// (policy_slab_row / policy_composition_bucket / policy_list_value).
type ruleFields struct {
	ThrVariable    string
	ThrOperator    string
	ThrValue       *float64
	ThrValueDate   *string
	ThrValueMode   string
	ThrPercentBase string
	ThrUnit        string

	SlabVariable    string
	SlabUnit        string
	SlabPercentBase string
	SlabRows        []slabRowConfig

	CompBase               string
	CompTotalCheckVariable string
	CompTotalCheckMin      *float64
	CompTotalCheckMax      *float64
	CompBuckets            []compositionBucketConfig

	ListTargetField   string
	ListMode          string
	ListSource        string
	ListDynamicRef    string
	ListCaseSensitive bool
	ListValues        []string

	FormulaExpression string
	FormulaReturnType string
	FormulaOperator   string
	FormulaValue      *float64
}

// parseRuleConfig decodes the rule-type-specific `config` payload into flat fields.
func parseRuleConfig(ruleType string, raw json.RawMessage) (ruleFields, error) {
	var out ruleFields
	if len(raw) == 0 {
		return out, fmt.Errorf("config is required for rule_type=%s", ruleType)
	}
	switch ruleType {
	case "threshold":
		var c thresholdConfig
		if err := json.Unmarshal(raw, &c); err != nil {
			return out, fmt.Errorf("invalid threshold config: %w", err)
		}
		out.ThrVariable = c.Variable
		out.ThrOperator = c.Operator
		if c.ValueDate != "" {
			d := c.ValueDate
			out.ThrValueDate = &d
		} else {
			v := c.Value
			out.ThrValue = &v
		}
		out.ThrValueMode = c.ValueMode
		out.ThrPercentBase = c.PercentBase
		out.ThrUnit = c.Unit
	case "slabs":
		var c slabsConfig
		if err := json.Unmarshal(raw, &c); err != nil {
			return out, fmt.Errorf("invalid slabs config: %w", err)
		}
		out.SlabVariable = c.Variable
		out.SlabUnit = c.Unit
		out.SlabPercentBase = c.PercentBase
		out.SlabRows = c.Rows
	case "composition":
		var c compositionConfig
		if err := json.Unmarshal(raw, &c); err != nil {
			return out, fmt.Errorf("invalid composition config: %w", err)
		}
		out.CompBase = c.Base
		out.CompBuckets = c.Buckets
		if c.TotalCheck != nil {
			out.CompTotalCheckVariable = c.TotalCheck.Variable
			out.CompTotalCheckMin = c.TotalCheck.Min
			out.CompTotalCheckMax = c.TotalCheck.Max
		}
	case "list":
		var c listConfig
		if err := json.Unmarshal(raw, &c); err != nil {
			return out, fmt.Errorf("invalid list config: %w", err)
		}
		out.ListTargetField = c.TargetField
		out.ListMode = c.Mode
		out.ListSource = c.ListSource
		out.ListDynamicRef = c.DynamicRef
		out.ListCaseSensitive = c.CaseSensitive
		out.ListValues = c.Values
	case "formula":
		var c formulaConfig
		if err := json.Unmarshal(raw, &c); err != nil {
			return out, fmt.Errorf("invalid formula config: %w", err)
		}
		out.FormulaExpression = c.Expression
		out.FormulaReturnType = c.ReturnType
		out.FormulaOperator = c.Operator
		v := c.Value
		out.FormulaValue = &v
	default:
		return out, fmt.Errorf("unsupported rule_type: %s", ruleType)
	}
	if err := validateRuleFields(ruleType, out); err != nil {
		return out, err
	}
	return out, nil
}

// validateRuleFields rejects incomplete rule configs that would ERROR (→ HardBlock)
// at evaluate time. Called from create/update after JSON decode.
func validateRuleFields(ruleType string, rf ruleFields) error {
	switch ruleType {
	case "threshold":
		if strings.TrimSpace(rf.ThrVariable) == "" {
			return fmt.Errorf("threshold: variable is required")
		}
		if strings.TrimSpace(rf.ThrOperator) == "" {
			return fmt.Errorf("threshold: operator is required")
		}
		if !validOperator(rf.ThrOperator) {
			return fmt.Errorf("threshold: invalid operator %q", rf.ThrOperator)
		}
		hasDate := rf.ThrValueDate != nil && strings.TrimSpace(*rf.ThrValueDate) != ""
		hasNum := rf.ThrValue != nil
		if !hasDate && !hasNum {
			return fmt.Errorf("threshold: value or valueDate is required")
		}
		mode := strings.TrimSpace(rf.ThrValueMode)
		if mode == "" {
			mode = "Absolute"
		}
		if mode == "PercentOf" && strings.TrimSpace(rf.ThrPercentBase) == "" {
			return fmt.Errorf("threshold: percentBase is required when valueMode is PercentOf")
		}
	case "slabs":
		if strings.TrimSpace(rf.SlabVariable) == "" {
			return fmt.Errorf("slabs: variable is required")
		}
		if len(rf.SlabRows) == 0 {
			return fmt.Errorf("slabs: at least one slab row is required")
		}
		hasPercentOf := false
		for i, row := range rf.SlabRows {
			if !validSlabAction(row.Action) {
				return fmt.Errorf("slabs: row %d invalid action %q", i+1, row.Action)
			}
			if row.To != nil && *row.To < row.From {
				return fmt.Errorf("slabs: row %d to (%v) is less than from (%v)", i+1, *row.To, row.From)
			}
			if strings.EqualFold(strings.TrimSpace(row.Mode), "PercentOf") {
				hasPercentOf = true
			}
		}
		if hasPercentOf && strings.TrimSpace(rf.SlabPercentBase) == "" {
			return fmt.Errorf("slabs: percentBase is required when any row mode is PercentOf")
		}
	case "composition":
		if len(rf.CompBuckets) < 2 {
			return fmt.Errorf("composition: at least 2 buckets are required")
		}
		for i, b := range rf.CompBuckets {
			if strings.TrimSpace(b.Variable) == "" {
				return fmt.Errorf("composition: bucket %d variable is required", i+1)
			}
			if b.Min == nil && b.Max == nil {
				return fmt.Errorf("composition: bucket %d needs min and/or max", i+1)
			}
			if b.Min != nil && b.Max != nil && *b.Min > *b.Max {
				return fmt.Errorf("composition: bucket %d min (%v) is greater than max (%v)", i+1, *b.Min, *b.Max)
			}
		}
		if strings.TrimSpace(rf.CompTotalCheckVariable) != "" {
			if rf.CompTotalCheckMin == nil && rf.CompTotalCheckMax == nil {
				return fmt.Errorf("composition: totalCheck needs min and/or max when variable is set")
			}
			if rf.CompTotalCheckMin != nil && rf.CompTotalCheckMax != nil && *rf.CompTotalCheckMin > *rf.CompTotalCheckMax {
				return fmt.Errorf("composition: totalCheck min is greater than max")
			}
		}
	case "list":
		if strings.TrimSpace(rf.ListTargetField) == "" {
			return fmt.Errorf("list: targetField is required")
		}
		if strings.TrimSpace(rf.ListMode) == "" {
			return fmt.Errorf("list: mode is required")
		}
		if rf.ListMode != "Include" && rf.ListMode != "Exclude" {
			return fmt.Errorf("list: mode must be Include or Exclude")
		}
		src := strings.TrimSpace(rf.ListSource)
		if src == "" {
			src = "Static"
		}
		if src != "Static" && src != "Dynamic" {
			return fmt.Errorf("list: listSource must be Static or Dynamic")
		}
		if src == "Static" {
			nonEmpty := 0
			for _, v := range rf.ListValues {
				if strings.TrimSpace(v) != "" {
					nonEmpty++
				}
			}
			if nonEmpty == 0 {
				return fmt.Errorf("list: at least one static value is required")
			}
		}
		if src == "Dynamic" && strings.TrimSpace(rf.ListDynamicRef) == "" {
			return fmt.Errorf("list: dynamicRef is required when listSource is Dynamic")
		}
	case "formula":
		if strings.TrimSpace(rf.FormulaExpression) == "" {
			return fmt.Errorf("formula: expression is required")
		}
		ret := strings.TrimSpace(rf.FormulaReturnType)
		if ret == "" {
			ret = "boolean"
		}
		if ret != "boolean" && ret != "number" {
			return fmt.Errorf("formula: returnType must be boolean or number")
		}
		if ret == "number" {
			if strings.TrimSpace(rf.FormulaOperator) == "" {
				return fmt.Errorf("formula: operator is required when returnType is number")
			}
			if !validOperator(rf.FormulaOperator) {
				return fmt.Errorf("formula: invalid operator %q", rf.FormulaOperator)
			}
			if rf.FormulaValue == nil {
				return fmt.Errorf("formula: value is required when returnType is number")
			}
		}
	default:
		return fmt.Errorf("unsupported rule_type: %s", ruleType)
	}
	return nil
}

func validOperator(op string) bool {
	switch strings.TrimSpace(op) {
	case "<", "<=", ">", ">=", "=", "!=":
		return true
	default:
		return false
	}
}

func validSlabAction(action string) bool {
	switch strings.TrimSpace(action) {
	case "", "AutoApprove", "SoftWarning", "TriggerApproval", "HardBlock":
		return true
	default:
		return false
	}
}
