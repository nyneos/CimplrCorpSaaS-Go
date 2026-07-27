package runtime

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Conflict severities for HardBlock threshold lane analysis.
const (
	ConflictImpossible = "impossible"  // no value can PASS all HardBlock thresholds → error
	ConflictKnifeEdge  = "knife_edge"  // only a single equality value can PASS → warn
)

// ThrConstraint is one HardBlock threshold that must PASS for a submit to succeed.
type ThrConstraint struct {
	PolicyCode string
	Variable   string
	Operator   string
	Value      float64
}

// ConflictFinding is one variable-level conflict in a lane.
type ConflictFinding struct {
	Severity    string   `json:"severity"` // impossible | knife_edge
	Variable    string   `json:"variable"`
	Message     string   `json:"message"`
	PolicyCodes []string `json:"policy_codes"`
}

// ConflictReport groups findings for create/update/test/check responses.
type ConflictReport struct {
	Findings []ConflictFinding `json:"findings"`
}

func (r ConflictReport) HasImpossible() bool {
	for _, f := range r.Findings {
		if f.Severity == ConflictImpossible {
			return true
		}
	}
	return false
}

func (r ConflictReport) Warnings() []ConflictFinding {
	out := make([]ConflictFinding, 0)
	for _, f := range r.Findings {
		if f.Severity == ConflictKnifeEdge {
			out = append(out, f)
		}
	}
	return out
}

func (r ConflictReport) Impossible() []ConflictFinding {
	out := make([]ConflictFinding, 0)
	for _, f := range r.Findings {
		if f.Severity == ConflictImpossible {
			out = append(out, f)
		}
	}
	return out
}

func (r ConflictReport) FirstImpossibleMessage() string {
	for _, f := range r.Findings {
		if f.Severity == ConflictImpossible {
			return f.Message
		}
	}
	return ""
}

// AnalyzeHardBlockThresholdConflicts intersects PASS-regions of HardBlock
// thresholds per CDM variable. SoftWarning / NotifyOnly are ignored (they do
// not make a lane "never possible").
func AnalyzeHardBlockThresholdConflicts(constraints []ThrConstraint) ConflictReport {
	byVar := map[string][]ThrConstraint{}
	for _, c := range constraints {
		v := strings.TrimSpace(c.Variable)
		op := strings.TrimSpace(c.Operator)
		if v == "" || op == "" {
			continue
		}
		byVar[v] = append(byVar[v], c)
	}
	report := ConflictReport{Findings: make([]ConflictFinding, 0)}
	vars := make([]string, 0, len(byVar))
	for v := range byVar {
		vars = append(vars, v)
	}
	sort.Strings(vars)
	for _, v := range vars {
		cs := byVar[v]
		if len(cs) < 2 {
			continue // single HardBlock cannot contradict itself across policies
		}
		sev, msg, codes := intersectThrConstraints(v, cs)
		if sev == "" {
			continue
		}
		report.Findings = append(report.Findings, ConflictFinding{
			Severity:    sev,
			Variable:    v,
			Message:     msg,
			PolicyCodes: codes,
		})
	}
	return report
}

// ConstraintsFromPolicySnapshots extracts HardBlock absolute numeric thresholds.
// Skips PercentOf / date thresholds (not modelled in v1).
func ConstraintsFromPolicySnapshots(policies []map[string]interface{}) []ThrConstraint {
	out := make([]ThrConstraint, 0)
	for _, p := range policies {
		if !isHardBlockAction(asString(p["action_on_breach"])) {
			continue
		}
		if !strings.EqualFold(asString(p["rule_type"]), "threshold") {
			continue
		}
		mode := strings.TrimSpace(asString(p["thr_value_mode"]))
		if mode == "PercentOf" {
			continue
		}
		if strings.TrimSpace(asString(p["thr_value_date"])) != "" {
			continue
		}
		variable := strings.TrimSpace(asString(p["thr_variable"]))
		op := strings.TrimSpace(asString(p["thr_operator"]))
		val, ok := asFloat(p["thr_value"])
		if !ok || variable == "" || op == "" {
			continue
		}
		code := strings.TrimSpace(asString(p["code"]))
		if code == "" {
			code = strings.TrimSpace(asString(p["policy_id"]))
		}
		out = append(out, ThrConstraint{
			PolicyCode: code,
			Variable:   variable,
			Operator:   op,
			Value:      val,
		})
	}
	return out
}

func isHardBlockAction(action string) bool {
	return strings.EqualFold(strings.TrimSpace(action), "HardBlock")
}

func asString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}

func asFloat(v interface{}) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int32:
		return float64(t), true
	case int64:
		return float64(t), true
	case uint64:
		return float64(t), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f, err == nil
	default:
		// encoding/json.Number
		if s, ok := v.(interface{ String() string }); ok {
			f, err := strconv.ParseFloat(s.String(), 64)
			return f, err == nil
		}
		return 0, false
	}
}

func intersectThrConstraints(variable string, cs []ThrConstraint) (severity, message string, codes []string) {
	codes = uniqueCodes(cs)
	lo := math.Inf(-1)
	hi := math.Inf(1)
	loClosed, hiClosed := false, false
	var eq *float64
	excludes := map[float64]struct{}{}

	for _, c := range cs {
		op := strings.TrimSpace(c.Operator)
		lim := c.Value
		switch op {
		case ">":
			if lim > lo {
				lo, loClosed = lim, false
			} else if lim == lo {
				loClosed = false
			}
		case ">=":
			if lim > lo {
				lo, loClosed = lim, true
			}
			// lim == lo with open bound stays open (stricter)
		case "<":
			if lim < hi {
				hi, hiClosed = lim, false
			} else if lim == hi {
				hiClosed = false
			}
		case "<=":
			if lim < hi {
				hi, hiClosed = lim, true
			}
		case "=":
			if eq != nil && *eq != lim {
				return ConflictImpossible,
					fmt.Sprintf("HardBlock thresholds on %s contradict (equality %v vs %v) — policies %s; no value can pass",
						variable, *eq, lim, strings.Join(codes, ", ")),
					codes
			}
			v := lim
			eq = &v
		case "!=":
			excludes[lim] = struct{}{}
		default:
			// unknown op — skip
		}
	}

	// Apply equality
	if eq != nil {
		v := *eq
		if !inRange(v, lo, hi, loClosed, hiClosed) {
			return ConflictImpossible,
				fmt.Sprintf("HardBlock thresholds on %s have empty PASS set (equality %v outside other bounds) — policies %s",
					variable, v, strings.Join(codes, ", ")),
				codes
		}
		if _, punch := excludes[v]; punch {
			return ConflictImpossible,
				fmt.Sprintf("HardBlock thresholds on %s require =%v and !=%v — policies %s; nothing can pass",
					variable, v, v, strings.Join(codes, ", ")),
				codes
		}
		return ConflictKnifeEdge,
			fmt.Sprintf("HardBlock thresholds on %s only allow exactly %v (knife-edge) — policies %s",
				variable, v, strings.Join(codes, ", ")),
			codes
	}

	// Interval feasibility
	if lo > hi {
		return ConflictImpossible,
			fmt.Sprintf("HardBlock thresholds on %s have empty PASS set (lower bound > upper) — policies %s",
				variable, strings.Join(codes, ", ")),
			codes
	}
	if lo == hi {
		if !(loClosed && hiClosed) {
			return ConflictImpossible,
				fmt.Sprintf("HardBlock thresholds on %s have empty PASS set (open bounds meet at %v) — policies %s",
					variable, lo, strings.Join(codes, ", ")),
				codes
		}
		if _, punch := excludes[lo]; punch {
			return ConflictImpossible,
				fmt.Sprintf("HardBlock thresholds on %s only candidate %v is excluded — policies %s; nothing can pass",
					variable, lo, strings.Join(codes, ", ")),
				codes
		}
		return ConflictKnifeEdge,
			fmt.Sprintf("HardBlock thresholds on %s only allow exactly %v (knife-edge) — policies %s",
				variable, lo, strings.Join(codes, ", ")),
			codes
	}

	// Non-degenerate interval: excludes only punch holes — still OK unless
	// somehow the entire continuum is excluded (impossible with finite != set).
	return "", "", codes
}

func inRange(v, lo, hi float64, loClosed, hiClosed bool) bool {
	if v < lo || v > hi {
		return false
	}
	if v == lo && !loClosed {
		return false
	}
	if v == hi && !hiClosed {
		return false
	}
	return true
}

func uniqueCodes(cs []ThrConstraint) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		code := strings.TrimSpace(c.PolicyCode)
		if code == "" {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}
