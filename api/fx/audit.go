package fx

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/fx/auditutil"
)

type fxAuditConfig struct {
	ActionTable       string
	ActionParentCol   string
	ExtraFilterCol    string
	DownloadTable     string
	DownloadParentCol string
	DownloadType      string
	Source            string
}

type fxAuditQueryAttempt struct {
	query      string
	args       []interface{}
	hasJSON    bool
	idAliasCol string
}

func NewFXAuditHandler(db *sql.DB, cfg fxAuditConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeFXAuditError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeFXAuditError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		parentID := strings.TrimSpace(fmt.Sprint(req[cfg.ActionParentCol]))
		if parentID == "" || parentID == "<nil>" {
			writeFXAuditError(w, http.StatusBadRequest, cfg.ActionParentCol+" is required")
			return
		}
		parentID = normalizeFXAuditParentID(cfg, parentID)

		payload := make([]map[string]interface{}, 0)
		parentWhere, args := buildFXAuditParentFilter(cfg.ActionParentCol, parentID)
		extraWhere := ""
		if cfg.ExtraFilterCol != "" {
			extraValue := strings.TrimSpace(fmt.Sprint(req[cfg.ExtraFilterCol]))
			if extraValue != "" && extraValue != "<nil>" {
				args = append(args, extraValue)
				extraWhere = fmt.Sprintf(" AND %s = $%d", cfg.ExtraFilterCol, len(args))
			}
		}
		actionRows, err := queryFXActionAudit(r, db, cfg, parentWhere, args, extraWhere)
		if err != nil {
			if cfg.Source != "FX_FORWARD_MTM" {
				writeFXAuditError(w, http.StatusInternalServerError, "failed to read FX audit history")
				return
			}
		}
		payload = append(payload, actionRows...)

		if cfg.Source == "FX_FORWARD_MTM" && len(actionRows) == 0 {
			if synthesized := synthesizeMTMAuditRow(r, db, parentID); synthesized != nil {
				payload = append(payload, synthesized)
			}
		}

		if strings.TrimSpace(cfg.DownloadTable) != "" {
			downloadRows, downloadErr := queryFXDownloadAudit(r, db, cfg, parentID)
			if downloadErr == nil {
				payload = append(payload, downloadRows...)
			}
		}

		fileAuditRows, fileAuditErr := queryFXAdditionalFileAudit(r, db, cfg, parentID)
		if fileAuditErr == nil {
			payload = append(payload, fileAuditRows...)
		}

		api.RespondEnvelopeSuccess(w, "FX audit history fetched successfully", map[string]interface{}{
			"audit_logs": payload,
		})
	}
}

func queryFXActionAudit(r *http.Request, db *sql.DB, cfg fxAuditConfig, parentWhere string, args []interface{}, extraWhere string) ([]map[string]interface{}, error) {
	attempts := []fxAuditQueryAttempt{
		{
			query: fmt.Sprintf(`
				SELECT action_id AS audit_row_id,
				       %s,
				       actiontype,
				       processing_status,
				       requested_by,
				       requested_at,
				       requested_ip,
				       checker_by,
				       checker_at,
				       checker_ip,
				       checker_comment,
				       reason,
				       old_values,
				       new_values,
				       change_summary
				FROM %s
				WHERE %s%s
				ORDER BY requested_at ASC, action_id ASC
			`, cfg.ActionParentCol, cfg.ActionTable, parentWhere, extraWhere),
			args:       args,
			hasJSON:    true,
			idAliasCol: "action_id",
		},
		{
			query: fmt.Sprintf(`
				SELECT action_id AS audit_row_id,
				       %s,
				       action_type,
				       processing_status,
				       requested_by,
				       requested_at,
				       requested_ip,
				       checker_by,
				       checker_at,
				       checker_ip,
				       checker_comment,
				       reason,
				       old_values,
				       new_values,
				       change_summary
				FROM %s
				WHERE %s%s
				ORDER BY requested_at ASC, action_id ASC
			`, cfg.ActionParentCol, cfg.ActionTable, parentWhere, extraWhere),
			args:       args,
			hasJSON:    true,
			idAliasCol: "action_id",
		},
		{
			query: fmt.Sprintf(`
				SELECT action_id AS audit_row_id,
				       %s,
				       actiontype,
				       processing_status,
				       requested_by,
				       requested_at,
				       requested_ip,
				       checker_by,
				       checker_at,
				       checker_ip,
				       checker_comment,
				       reason
				FROM %s
				WHERE %s%s
				ORDER BY requested_at ASC, action_id ASC
			`, cfg.ActionParentCol, cfg.ActionTable, parentWhere, extraWhere),
			args:       args,
			hasJSON:    false,
			idAliasCol: "action_id",
		},
		{
			query: fmt.Sprintf(`
				SELECT action_id AS audit_row_id,
				       %s,
				       action_type,
				       processing_status,
				       requested_by,
				       requested_at,
				       requested_ip,
				       checker_by,
				       checker_at,
				       checker_ip,
				       checker_comment,
				       reason
				FROM %s
				WHERE %s%s
				ORDER BY requested_at ASC, action_id ASC
			`, cfg.ActionParentCol, cfg.ActionTable, parentWhere, extraWhere),
			args:       args,
			hasJSON:    false,
			idAliasCol: "action_id",
		},
		{
			query: fmt.Sprintf(`
				SELECT audit_id AS audit_row_id,
				       %s,
				       actiontype,
				       processing_status,
				       requested_by,
				       requested_at,
				       requested_ip,
				       checker_by,
				       checker_at,
				       checker_ip,
				       checker_comment,
				       reason,
				       old_values,
				       new_values,
				       change_summary
				FROM %s
				WHERE %s%s
				ORDER BY requested_at ASC, audit_id ASC
			`, cfg.ActionParentCol, cfg.ActionTable, parentWhere, extraWhere),
			args:       args,
			hasJSON:    true,
			idAliasCol: "audit_id",
		},
		{
			query: fmt.Sprintf(`
				SELECT audit_id AS audit_row_id,
				       %s,
				       action_type,
				       processing_status,
				       requested_by,
				       requested_at,
				       requested_ip,
				       checker_by,
				       checker_at,
				       checker_ip,
				       checker_comment,
				       reason,
				       old_values,
				       new_values,
				       change_summary
				FROM %s
				WHERE %s%s
				ORDER BY requested_at ASC, audit_id ASC
			`, cfg.ActionParentCol, cfg.ActionTable, parentWhere, extraWhere),
			args:       args,
			hasJSON:    true,
			idAliasCol: "audit_id",
		},
		{
			query: fmt.Sprintf(`
				SELECT audit_id AS audit_row_id,
				       %s,
				       actiontype,
				       processing_status,
				       requested_by,
				       requested_at,
				       requested_ip,
				       checker_by,
				       checker_at,
				       checker_ip,
				       checker_comment,
				       reason
				FROM %s
				WHERE %s%s
				ORDER BY requested_at ASC, audit_id ASC
			`, cfg.ActionParentCol, cfg.ActionTable, parentWhere, extraWhere),
			args:       args,
			hasJSON:    false,
			idAliasCol: "audit_id",
		},
		{
			query: fmt.Sprintf(`
				SELECT audit_id AS audit_row_id,
				       %s,
				       action_type,
				       processing_status,
				       requested_by,
				       requested_at,
				       requested_ip,
				       checker_by,
				       checker_at,
				       checker_ip,
				       checker_comment,
				       reason
				FROM %s
				WHERE %s%s
				ORDER BY requested_at ASC, audit_id ASC
			`, cfg.ActionParentCol, cfg.ActionTable, parentWhere, extraWhere),
			args:       args,
			hasJSON:    false,
			idAliasCol: "audit_id",
		},
	}

	var lastErr error
	for _, attempt := range attempts {
		rows, err := db.QueryContext(r.Context(), attempt.query, attempt.args...)
		if err != nil {
			lastErr = err
			continue
		}

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			entry, scanErr := scanFXAuditRow(rows, attempt.hasJSON)
			if scanErr != nil {
				rows.Close()
				return nil, scanErr
			}
			if shouldHideFXAuditActionRow(cfg.Source, entry) {
				continue
			}
			entry["source"] = cfg.Source
			entry["id_column"] = attempt.idAliasCol
			payload = append(payload, entry)
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			return nil, rowsErr
		}
		return payload, nil
	}

	return nil, lastErr
}

func shouldHideFXAuditActionRow(source string, entry map[string]interface{}) bool {
	switch source {
	case "FX_FORWARD_CANCELLATION", "FX_FORWARD_ROLLOVER":
		return strings.EqualFold(strings.TrimSpace(fmt.Sprint(entry["action_type"])), constants.AuditActionReject)
	default:
		return false
	}
}

func buildFXAuditParentFilter(columnName, parentID string) (string, []interface{}) {
	parentID = strings.TrimSpace(parentID)
	legacyByteString := strings.TrimSpace(fmt.Sprint([]byte(parentID)))
	if parentID == "" {
		return fmt.Sprintf("%s = $1", columnName), []interface{}{parentID}
	}
	if legacyByteString == "" || legacyByteString == parentID {
		return fmt.Sprintf("%s = $1", columnName), []interface{}{parentID}
	}
	return fmt.Sprintf("(%s = $1 OR %s = $2)", columnName, columnName), []interface{}{parentID, legacyByteString}
}

func normalizeFXAuditParentID(cfg fxAuditConfig, parentID string) string {
	parentID = strings.TrimSpace(parentID)
	if parentID == "" {
		return parentID
	}

	switch cfg.Source {
	case "FX_FORWARD_MTM":
		if decoded, err := base64.StdEncoding.DecodeString(parentID); err == nil {
			if normalized := strings.TrimSpace(string(decoded)); normalized != "" {
				return normalized
			}
		}
	}

	return parentID
}

func synthesizeMTMAuditRow(r *http.Request, db *sql.DB, parentID string) map[string]interface{} {
	row := auditutil.FetchRowSnapshot(r.Context(), db, "public.forward_mtm", "mtm_id", parentID)
	if len(row) == 0 {
		return nil
	}

	requestedBy := firstNonBlankString(
		row["created_by"],
		row["uploaded_by"],
		row["user_id"],
		row["entity"],
	)
	if requestedBy == "" {
		requestedBy = "system"
	}

	requestedAt := firstNonZeroTime(row["calculated_at"], row["created_at"], row["deal_date"])
	status := firstNonBlankString(row["processing_status"], row["status"])
	if status == "" {
		status = constants.StatusPendingApproval
	}

	entry := map[string]interface{}{
		"audit_id":          "synthetic-create-" + parentID,
		"action_id":         "synthetic-create-" + parentID,
		"entity_id":         parentID,
		"action_type":       "CREATE",
		"processing_status": status,
		"requested_by":      requestedBy,
		"requested_at":      requestedAt,
		"requested_ip":      "",
		"checker_by":        "",
		"checker_at":        nil,
		"checker_ip":        "",
		"checker_comment":   "",
		"reason":            "Synthesized from forward_mtm because no MTM audit action row was found.",
		"source":            "FX_FORWARD_MTM",
	}

	if uploadKey := firstNonBlankString(row["upload_s3_key"]); uploadKey != "" {
		entry["upload_s3_key"] = uploadKey
		entry["file_name"] = auditutil.FileName(uploadKey)
	}

	if summary := buildSyntheticChangeSummary(row); len(summary) > 0 {
		entry["change_summary"] = summary
	}

	return entry
}

func buildSyntheticChangeSummary(row map[string]interface{}) []map[string]interface{} {
	keys := []string{
		"booking_id",
		"internal_reference_id",
		"entity",
		"currency_pair",
		"buy_sell",
		"notional_amount",
		"contract_rate",
		"mtm_rate",
		"mtm_value",
		"status",
		"upload_s3_key",
	}
	sort.Strings(keys)

	changes := make([]map[string]interface{}, 0, len(keys))
	for _, key := range keys {
		value, ok := row[key]
		if !ok || value == nil || strings.TrimSpace(fmt.Sprint(value)) == "" {
			continue
		}
		changes = append(changes, map[string]interface{}{
			"field":     key,
			"old_value": nil,
			"new_value": value,
		})
	}
	return changes
}

func firstNonBlankString(values ...interface{}) string {
	for _, value := range values {
		text := strings.TrimSpace(fmt.Sprint(value))
		if text != "" && text != "<nil>" {
			return text
		}
	}
	return ""
}

func firstNonZeroTime(values ...interface{}) interface{} {
	for _, value := range values {
		switch v := value.(type) {
		case time.Time:
			if !v.IsZero() {
				return api.FormatAuditTimestampIST(v)
			}
		case string:
			text := strings.TrimSpace(v)
			if text != "" {
				return text
			}
		case []byte:
			text := strings.TrimSpace(string(v))
			if text != "" {
				return text
			}
		}
	}
	return nil
}

func queryFXAdditionalFileAudit(r *http.Request, db *sql.DB, cfg fxAuditConfig, parentID string) ([]map[string]interface{}, error) {
	moduleKeys := fxAdditionalFileModules(cfg.Source)
	if len(moduleKeys) == 0 {
		return nil, nil
	}

	placeholders := make([]string, 0, len(moduleKeys))
	args := []interface{}{parentID}
	for _, moduleKey := range moduleKeys {
		args = append(args, moduleKey)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
	}

	query := fmt.Sprintf(`
		SELECT parent_record_id,
		       file_id,
		       module_key,
		       action_type,
		       processing_status,
		       requested_by,
		       requested_at,
		       requested_ip,
		       checker_by,
		       checker_at,
		       checker_ip,
		       checker_comment,
		       reason
		FROM cimplrcorpsaas.fx_additional_file_audit
		WHERE parent_record_id = $1
		  AND module_key IN (%s)
		ORDER BY requested_at ASC, audit_id ASC
	`, strings.Join(placeholders, ", "))

	rows, err := db.QueryContext(r.Context(), query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	payload := make([]map[string]interface{}, 0)
	for rows.Next() {
		var parentRecordID, fileID, moduleKey string
		var actionType, status, requestedBy, requestedIP, checkerBy, checkerIP, checkerComment, reason sql.NullString
		var requestedAt, checkerAt sql.NullTime
		if err := rows.Scan(&parentRecordID, &fileID, &moduleKey, &actionType, &status, &requestedBy, &requestedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &checkerComment, &reason); err != nil {
			return nil, err
		}
		payload = append(payload, map[string]interface{}{
			"entity_id":         parentRecordID,
			"file_id":           strings.TrimSpace(fileID),
			"module_key":        strings.TrimSpace(moduleKey),
			"action_type":       nullString(actionType),
			"processing_status": nullString(status),
			"requested_by":      nullString(requestedBy),
			"requested_at":      nullTime(requestedAt),
			"requested_ip":      nullString(requestedIP),
			"checker_by":        nullString(checkerBy),
			"checker_at":        nullTime(checkerAt),
			"checker_ip":        nullString(checkerIP),
			"checker_comment":   nullString(checkerComment),
			"reason":            nullString(reason),
			"source":            cfg.Source,
		})
	}
	return payload, rows.Err()
}

func fxAdditionalFileModules(source string) []string {
	switch source {
	case "FX_EXPOSURE":
		return []string{"fx-exposure"}
	case "FX_EXPOSURE_BUCKETING":
		return []string{"fx-exposure-bucketing", "fx-pending-exposure-bucketing"}
	case "FX_FORWARD":
		return []string{"fx-forward"}
	case "FX_FORWARD_MTM":
		return []string{"fx-mtm"}
	default:
		return nil
	}
}

func scanFXAuditRow(rows *sql.Rows, hasJSON bool) (map[string]interface{}, error) {
	var (
		actionID       interface{}
		entityID       string
		action         sql.NullString
		status         sql.NullString
		requestedBy    sql.NullString
		requestedAt    sql.NullTime
		requestedIP    sql.NullString
		checkerBy      sql.NullString
		checkerAt      sql.NullTime
		checkerIP      sql.NullString
		checkerComment sql.NullString
		reason         sql.NullString
		oldValues      sql.NullString
		newValues      sql.NullString
		changeSummary  sql.NullString
	)
	if hasJSON {
		if err := rows.Scan(&actionID, &entityID, &action, &status, &requestedBy, &requestedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &checkerComment, &reason, &oldValues, &newValues, &changeSummary); err != nil {
			return nil, err
		}
	} else if err := rows.Scan(&actionID, &entityID, &action, &status, &requestedBy, &requestedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &checkerComment, &reason); err != nil {
		return nil, err
	}
	auditID := normalizeFXAuditID(actionID)
	entry := map[string]interface{}{
		"audit_id":          auditID,
		"action_id":         auditID,
		"entity_id":         entityID,
		"action_type":       nullString(action),
		"processing_status": nullString(status),
		"requested_by":      nullString(requestedBy),
		"requested_at":      nullTime(requestedAt),
		"requested_ip":      nullString(requestedIP),
		"checker_by":        nullString(checkerBy),
		"checker_at":        nullTime(checkerAt),
		"checker_ip":        nullString(checkerIP),
		"checker_comment":   nullString(checkerComment),
		"reason":            nullString(reason),
	}
	if value := parseJSONColumn(changeSummary); value != nil {
		entry["change_summary"] = value
	}
	if value := parseJSONColumn(oldValues); value != nil {
		entry["old_values"] = value
	}
	if value := parseJSONColumn(newValues); value != nil {
		entry["new_values"] = value
	}
	return entry, nil
}

func normalizeFXAuditID(value interface{}) interface{} {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		text := strings.TrimSpace(string(v))
		if text == "" {
			return nil
		}
		return text
	default:
		text := strings.TrimSpace(fmt.Sprint(v))
		if text == "" || text == "<nil>" {
			return nil
		}
		return text
	}
}

func queryFXDownloadAudit(r *http.Request, db *sql.DB, cfg fxAuditConfig, parentID string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT %s, requested_by, requested_at, requested_ip, file_name, upload_s3_key
		FROM %s
		WHERE %s = $1
		ORDER BY requested_at ASC, download_audit_id ASC
	`, cfg.DownloadParentCol, cfg.DownloadTable, cfg.DownloadParentCol)
	rows, err := db.QueryContext(r.Context(), query, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	payload := make([]map[string]interface{}, 0)
	for rows.Next() {
		var entityID, requestedBy string
		var requestedAt sql.NullTime
		var requestedIP, fileName, uploadKey sql.NullString
		if err := rows.Scan(&entityID, &requestedBy, &requestedAt, &requestedIP, &fileName, &uploadKey); err != nil {
			return nil, err
		}
		entry := map[string]interface{}{
			"entity_id":       entityID,
			"action_type":     "DOWNLOAD",
			"requested_by":    strings.TrimSpace(requestedBy),
			"requested_at":    nullTime(requestedAt),
			"requested_ip":    nullString(requestedIP),
			"checker_by":      "",
			"checker_at":      nil,
			"checker_ip":      "",
			"checker_comment": "",
			"reason":          "",
			"file_name":       nullString(fileName),
			"upload_s3_key":   nullString(uploadKey),
			"download_type":   cfg.DownloadType,
			"source":          cfg.Source,
		}
		payload = append(payload, entry)
	}
	return payload, rows.Err()
}

func writeFXAuditError(w http.ResponseWriter, status int, message string) {
	api.LogError("%s", message)
	api.RespondEnvelopeError(w, status, message, api.EnvelopeErrorCode(status))
}

func nullString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func nullTime(value sql.NullTime) interface{} {
	return api.FormatAuditTimestampNullIST(value)
}

func parseJSONColumn(value sql.NullString) interface{} {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil
	}
	var parsed interface{}
	if err := json.Unmarshal([]byte(value.String), &parsed); err != nil {
		return value.String
	}
	return parsed
}

func fxExposureAuditConfig() fxAuditConfig {
	return fxAuditConfig{ActionTable: auditutil.TableExposure, ActionParentCol: "exposure_header_id", DownloadTable: auditutil.TableExposureDownloads, DownloadParentCol: "exposure_header_id", Source: "FX_EXPOSURE"}
}

func fxBucketingAuditConfig() fxAuditConfig {
	return fxAuditConfig{ActionTable: auditutil.TableExposureBucketing, ActionParentCol: "exposure_header_id", Source: "FX_EXPOSURE_BUCKETING"}
}

func fxHedgeProposalAuditConfig() fxAuditConfig {
	return fxAuditConfig{ActionTable: auditutil.TableHedgeProposal, ActionParentCol: "exposure_header_id", Source: "FX_HEDGE_PROPOSAL"}
}

func fxHedgeLinkAuditConfig() fxAuditConfig {
	return fxAuditConfig{ActionTable: auditutil.TableHedgeLink, ActionParentCol: "exposure_header_id", ExtraFilterCol: "booking_id", Source: "FX_HEDGE_LINK"}
}

func fxForwardAuditConfig() fxAuditConfig {
	return fxAuditConfig{ActionTable: auditutil.TableForwardBooking, ActionParentCol: "system_transaction_id", DownloadTable: auditutil.TableForwardDownloads, DownloadParentCol: "system_transaction_id", DownloadType: "BOOKING", Source: "FX_FORWARD"}
}

func fxMTMAuditConfig() fxAuditConfig {
	return fxAuditConfig{ActionTable: auditutil.TableForwardMTM, ActionParentCol: "mtm_id", DownloadTable: auditutil.TableForwardMTMDownloads, DownloadParentCol: "mtm_id", Source: "FX_FORWARD_MTM"}
}

func fxCancellationAuditConfig() fxAuditConfig {
	return fxAuditConfig{ActionTable: auditutil.TableForwardCancellation, ActionParentCol: "booking_id", Source: "FX_FORWARD_CANCELLATION"}
}

func fxRolloverAuditConfig() fxAuditConfig {
	return fxAuditConfig{ActionTable: auditutil.TableForwardRollover, ActionParentCol: "booking_id", Source: "FX_FORWARD_ROLLOVER"}
}
