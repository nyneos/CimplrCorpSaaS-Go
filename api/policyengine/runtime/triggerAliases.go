package runtime

import (
	"strings"

	"CimplrCorpSaas/api/policyengine/common"
)

// ExpandTriggerAliases returns the event code plus synonyms so policies authored
// against BRD codes (PRE_SAVE, PRE_SUBMIT, …) still match CRUD RunCheck hooks.
//
// Rules (tightened vs early pilot):
//   - PRE_CREATE matches PRE_SAVE + PRE_SUBMIT (create / maker-submit paths)
//   - PRE_EDIT matches PRE_SAVE only (not PRE_SUBMIT)
//   - PRE_UPLOAD / POST_UPLOAD match ON_DATA_IMPORT
//   - POST_CREATE matches POST_SAVE
//   - PRE_APPROVE / PRE_REJECT optionally match WORKFLOW_TRANSITION
//   - POST_APPROVE / POST_REJECT match WORKFLOW_TRANSITION + ON_STATUS_CHANGE
//   - POST_TRADE, ON_FIELD_CHANGE, SCHEDULED_* stand alone (no CRUD fold)
func ExpandTriggerAliases(eventCode string) []string {
	code := strings.ToUpper(strings.TrimSpace(eventCode))
	if code == "" {
		return []string{}
	}
	aliases := map[string][]string{
		common.TriggerPreCreate: {
			common.TriggerPreCreate, common.TriggerPreSubmit, common.TriggerPreSave,
		},
		common.TriggerPostCreate: {
			common.TriggerPostCreate, common.TriggerPostSave,
		},
		common.TriggerPreUpload: {
			common.TriggerPreUpload, common.TriggerOnDataImport,
		},
		common.TriggerPostUpload: {
			common.TriggerPostUpload, common.TriggerOnDataImport,
		},
		common.TriggerPreEdit: {
			common.TriggerPreEdit, common.TriggerPreSave,
		},
		common.TriggerPostEdit: {common.TriggerPostEdit},
		common.TriggerPreDelete: {
			common.TriggerPreDelete, common.TriggerPreSave,
		},
		common.TriggerPostDelete: {
			common.TriggerPostDelete, common.TriggerPostSave,
		},
		common.TriggerPreApprove: {
			common.TriggerPreApprove, common.TriggerWorkflowTransition,
		},
		common.TriggerPostApprove: {
			common.TriggerPostApprove, common.TriggerWorkflowTransition, common.TriggerOnStatusChange,
		},
		common.TriggerPreReject: {
			common.TriggerPreReject, common.TriggerWorkflowTransition,
		},
		common.TriggerPostReject: {
			common.TriggerPostReject, common.TriggerWorkflowTransition, common.TriggerOnStatusChange,
		},
		// Legacy → include canonical CRUD
		common.TriggerPreSubmit: {
			common.TriggerPreSubmit, common.TriggerPreCreate, common.TriggerPreSave,
		},
		common.TriggerPreSave: {
			common.TriggerPreSave, common.TriggerPreCreate, common.TriggerPreEdit, common.TriggerPreDelete,
		},
		common.TriggerPostSave: {
			common.TriggerPostSave, common.TriggerPostCreate, common.TriggerPostDelete, common.TriggerPostUpload,
		},
		common.TriggerOnDataImport: {
			common.TriggerOnDataImport, common.TriggerPreUpload, common.TriggerPostUpload,
		},
		common.TriggerWorkflowTransition: {
			common.TriggerWorkflowTransition,
			common.TriggerPreApprove, common.TriggerPreReject,
			common.TriggerPostApprove, common.TriggerPostReject,
		},
		common.TriggerOnFieldChange:      {common.TriggerOnFieldChange},
		common.TriggerPostTrade:          {common.TriggerPostTrade},
		common.TriggerScheduledDaily:     {common.TriggerScheduledDaily},
		common.TriggerScheduledIntraday:  {common.TriggerScheduledIntraday},
		common.TriggerScheduledMonthly:   {common.TriggerScheduledMonthly},
		common.TriggerOnStatusChange:     {common.TriggerOnStatusChange},
	}
	if list, ok := aliases[code]; ok {
		return list
	}
	return []string{code}
}
