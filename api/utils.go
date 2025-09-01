package api

import "database/sql"

// ActionAuditInfo holds audit info for a record
type ActionAuditInfo struct {
	CreatedBy string
	CreatedAt string
	EditedBy  string
	EditedAt  string
	DeletedBy string
	DeletedAt string
}

// GetAuditInfo parses audit action fields and returns audit info for create/edit
func GetAuditInfo(actionType string, requestedBy sql.NullString, requestedAt sql.NullTime) ActionAuditInfo {
	info := ActionAuditInfo{}
	switch actionType {
	case "CREATE":
		info.CreatedBy = getNullString(requestedBy)
		info.CreatedAt = getNullTime(requestedAt)
	case "EDIT":
		info.EditedBy = getNullString(requestedBy)
		info.EditedAt = getNullTime(requestedAt)
	case "DELETE":
		info.DeletedBy = getNullString(requestedBy)
		info.DeletedAt = getNullTime(requestedAt)
	}
	return info
}

// getNullString returns empty string for invalid sql.NullString
func getNullString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

// getNullTime returns formatted time for valid sql.NullTime
func getNullTime(nt sql.NullTime) string {
	if nt.Valid {
		return nt.Time.Format("2006-01-02 15:04:05")
	}
	return ""
}
