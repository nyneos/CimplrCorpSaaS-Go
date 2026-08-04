package approvalengine

import (
	"CimplrCorpSaas/api/constants"
	"time"
)

// ─── Status / Action constants ────────────────────────────────────────────────

// Instance statuses
const (
	InstStatusPending   = "PENDING"
	InstStatusApproved  = constants.StatusApproved
	InstStatusRejected  = constants.StatusRejected
	InstStatusCancelled = "CANCELLED"
	InstStatusExpired   = "EXPIRED"
)

// Eye statuses
const (
	EyeStatusWaiting   = "WAITING"
	EyeStatusActive    = constants.StatusActive
	EyeStatusApproved  = constants.StatusApproved
	EyeStatusRejected  = constants.StatusRejected
	EyeStatusEscalated = "ESCALATED"
	EyeStatusSkipped   = "SKIPPED"
)

// Action types recorded in approval_instance_action
const (
	ActionApproved      = constants.StatusApproved
	ActionRejected      = constants.StatusRejected
	ActionEscalated     = "ESCALATED"
	ActionAutoEscalated = "AUTO_ESCALATED"
	ActionRecalled      = "RECALLED"
)

// Approval order modes from approval_matrix_master
const (
	OrderSequential = "SEQUENTIAL"
	OrderParallel   = "PARALLEL"
)

// ─── Request / Input types ────────────────────────────────────────────────────

// InstanceRequest is passed by calling modules to CreateInstance.
type InstanceRequest struct {
	ModuleCode       string
	EntityCode       string
	TransactionType  string
	RecordID         string
	RecordTable      string // e.g. "investment.fd_tds_plan_master"
	AuditTable       string // e.g. "investment.fd_audit_tds_plan"
	AuditIDColumn    string // PK col in BOTH master and audit table, e.g. "tds_plan_id"
	ActionType       string // CREATE / EDIT / DELETE
	Amount           float64
	SubmittedBy      string // public.users.id
	SubmittedByEmail string
	MatrixID string
}

// ActionRequest is passed to RecordAction when an approver acts on an eye.
type ActionRequest struct {
	InstanceEyeID string
	ActorUserID   string
	ActorEmail    string
	ActorRoleID   string // empty string if not acting under a specific role
	ActionType    string // APPROVED or REJECTED
	Comment       string
}

// ─── Internal / resolver types ────────────────────────────────────────────────

// MatrixEye holds data for one eye (round) returned by ResolveMatrix.
type MatrixEye struct {
	EyeID             string
	Position          int
	EyeCount          int
	ApprovalsRequired int
	SlaHours          *int
	EscalationUserID  *string
	EscalationRoleID  *string
}

// MatrixResult is returned by ResolveMatrix.
type MatrixResult struct {
	MatrixID      string
	ApprovalOrder string
	SlaHours      *int
	Eyes          []MatrixEye
}

// ─── Query / response types ───────────────────────────────────────────────────

// InstanceSummary is returned by GetPendingForUser.
type InstanceSummary struct {
	InstanceID        string
	ModuleCode        string
	TransactionType   string
	RecordID          string
	ActionType        string
	Status            string
	SubmittedByEmail  string
	SubmittedAt       time.Time
	CurrentEyeID      string
	CurrentPosition   int
	ApprovalsRequired int
	ApprovalsReceived int
	SlaDeadline       *time.Time
	IsEscalated       bool
}

// EyeActionDetail is one approve/reject/escalate event within an eye.
type EyeActionDetail struct {
	ActionID    string
	ActorEmail  string
	ActorRoleID string
	ActionType  string
	Comment     string
	ActedAt     time.Time
	IsSystem    bool
}

// EyeDetail is the full state of one eye including all actions taken.
type EyeDetail struct {
	InstanceEyeID     string
	MatrixEyeID       string
	Position          int
	EyeCount          int
	ApprovalsRequired int
	ApprovalsReceived int
	Status            string
	ActivatedAt       *time.Time
	SlaDeadline       *time.Time
	ResolvedAt        *time.Time
	IsEscalated       bool
	EscalatedAt       *time.Time
	Actions           []EyeActionDetail
}

// InstanceDetail is the full state of one instance including all eyes.
type InstanceDetail struct {
	InstanceID         string
	MatrixID           string
	ModuleCode         string
	EntityCode         string
	TransactionType    string
	RecordID           string
	RecordTable        string
	ActionType         string
	Status             string
	SubmittedByEmail   string
	SubmittedAt        time.Time
	OverallSlaHours    *int
	OverallSlaDeadline *time.Time
	ResolvedAt         *time.Time
	ResolvedByEmail    string
	Eyes               []EyeDetail
}

// ─── Rich / enriched response types ──────────────────────────────────────────

type ApproverInfo struct {
	SlotOrder      int    `json:"slot_order"`
	MemberType     string `json:"member_type"`
	AssignmentType string `json:"assignment_type"` // USER_ONLY | ROLE_ONLY | ROLE_USER
	UserID         string `json:"user_id,omitempty"`
	UserEmail      string `json:"user_email,omitempty"`
	UserName       string `json:"user_name,omitempty"`
	RoleID         string `json:"role_id,omitempty"`
	RoleName       string `json:"role_name,omitempty"`
	// Action taken by this approver on this eye (empty if not yet acted).
	ActionTaken   string     `json:"action_taken,omitempty"` // APPROVED | REJECTED | ESCALATED | ...
	ActionAt      *time.Time `json:"action_at,omitempty"`
	ActionComment string     `json:"action_comment,omitempty"`
}

// RichEyeDetail is an eye enriched with numbered approver details.
type RichEyeDetail struct {
	InstanceEyeID     string     `json:"instance_eye_id"`
	Position          int        `json:"position"`
	EyeCount          int        `json:"eye_count"`
	ApprovalsRequired int        `json:"approvals_required"`
	ApprovalsReceived int        `json:"approvals_received"`
	Status            string     `json:"status"`
	ActivatedAt       *time.Time `json:"activated_at,omitempty"`
	SlaDeadline       *time.Time `json:"sla_deadline,omitempty"`
	ResolvedAt        *time.Time `json:"resolved_at,omitempty"`
	IsEscalated       bool       `json:"is_escalated"`
	EscalatedAt       *time.Time `json:"escalated_at,omitempty"`
	// Approvers: dynamically keyed "approver_1", "approver_2", ...
	Approvers map[string]ApproverInfo `json:"approvers"`
	// Full action log for this eye (all actions, including system).
	ActionLog []EyeActionDetail `json:"action_log"`
}

// RichInstanceDetail is GetInstanceDetail enriched with per-eye approver identities.
type RichInstanceDetail struct {
	InstanceID         string     `json:"instance_id"`
	MatrixID           string     `json:"matrix_id"`
	ModuleCode         string     `json:"module_code"`
	EntityCode         string     `json:"entity_code"`
	TransactionType    string     `json:"transaction_type"`
	RecordID           string     `json:"record_id"`
	RecordTable        string     `json:"record_table"`
	ActionType         string     `json:"action_type"`
	Status             string     `json:"status"`
	SubmittedByEmail   string     `json:"submitted_by_email"`
	SubmittedAt        time.Time  `json:"submitted_at"`
	OverallSlaHours    *int       `json:"overall_sla_hours,omitempty"`
	OverallSlaDeadline *time.Time `json:"overall_sla_deadline,omitempty"`
	ResolvedAt         *time.Time `json:"resolved_at,omitempty"`
	ResolvedByEmail    string     `json:"resolved_by_email,omitempty"`
	// ViewerCanAct is true when the viewing user has an active eye they can act on.
	ViewerCanAct bool `json:"viewer_can_act"`
	// ViewerActiveEyeID is the instance_eye_id the viewer should act on, if any.
	ViewerActiveEyeID string          `json:"viewer_active_eye_id,omitempty"`
	Eyes              []RichEyeDetail `json:"eyes"`
}
