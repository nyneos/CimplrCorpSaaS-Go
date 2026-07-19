package policy

import (
	"encoding/json"
	"fmt"
)

// The shapes below mirror the frontend's RuleConfig union (PolicyEngine/types.ts)
// exactly by JSON key so `config` can be decoded straight off the wire.

type thresholdConfig struct {
	Variable    string  `json:"variable"`
	Operator    string  `json:"operator"`
	Value       float64 `json:"value"`
	ValueMode   string  `json:"valueMode"`
	PercentBase string  `json:"percentBase"`
	Unit        string  `json:"unit"`
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
	Variable string          `json:"variable"`
	Rows     []slabRowConfig `json:"rows"`
	Unit     string          `json:"unit"`
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
	ThrValueMode   string
	ThrPercentBase string
	ThrUnit        string

	SlabVariable string
	SlabUnit     string
	SlabRows     []slabRowConfig

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
		v := c.Value
		out.ThrValue = &v
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
	return out, nil
}
