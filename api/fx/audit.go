package fx

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

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

func NewFXAuditHandler(db *sql.DB, cfg fxAuditConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
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

		payload := make([]map[string]interface{}, 0)
		args := []interface{}{parentID}
		extraWhere := ""
		if cfg.ExtraFilterCol != "" {
			extraValue := strings.TrimSpace(fmt.Sprint(req[cfg.ExtraFilterCol]))
			if extraValue != "" && extraValue != "<nil>" {
				args = append(args, extraValue)
				extraWhere = fmt.Sprintf(" AND %s = $%d", cfg.ExtraFilterCol, len(args))
			}
		}
		query := fmt.Sprintf(`
			SELECT action_id,
			       %s,
			       actiontype,
			       processing_status,
			       requested_by,
			       requested_at,
			       checker_by,
			       checker_at,
			       checker_comment,
			       reason,
			       old_values,
			       new_values,
			       change_summary
			FROM %s
			WHERE %s = $1%s
			ORDER BY requested_at ASC, action_id ASC
		`, cfg.ActionParentCol, cfg.ActionTable, cfg.ActionParentCol, extraWhere)

		rows, err := db.QueryContext(r.Context(), query, args...)
		if err != nil {
			query = fmt.Sprintf(`
				SELECT action_id,
				       %s,
				       actiontype,
				       processing_status,
				       requested_by,
				       requested_at,
				       checker_by,
				       checker_at,
				       checker_comment,
				       reason
				FROM %s
				WHERE %s = $1%s
				ORDER BY requested_at ASC, action_id ASC
			`, cfg.ActionParentCol, cfg.ActionTable, cfg.ActionParentCol, extraWhere)
			rows, err = db.QueryContext(r.Context(), query, args...)
		}
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				entry, scanErr := scanFXAuditRow(rows)
				if scanErr != nil {
					writeFXAuditError(w, http.StatusInternalServerError, "failed to read FX audit history")
					return
				}
				entry["source"] = cfg.Source
				payload = append(payload, entry)
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				writeFXAuditError(w, http.StatusInternalServerError, "failed to read FX audit history")
				return
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

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"audit_logs":           payload,
		})
	}
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
		       checker_by,
		       checker_at,
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
		var actionType, status, requestedBy, checkerBy, checkerComment, reason sql.NullString
		var requestedAt, checkerAt sql.NullTime
		if err := rows.Scan(&parentRecordID, &fileID, &moduleKey, &actionType, &status, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
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
			"checker_by":        nullString(checkerBy),
			"checker_at":        nullTime(checkerAt),
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

func scanFXAuditRow(rows *sql.Rows) (map[string]interface{}, error) {
	var (
		actionID       int64
		entityID       string
		action         sql.NullString
		status         sql.NullString
		requestedBy    sql.NullString
		requestedAt    sql.NullTime
		checkerBy      sql.NullString
		checkerAt      sql.NullTime
		checkerComment sql.NullString
		reason         sql.NullString
		oldValues      sql.NullString
		newValues      sql.NullString
		changeSummary  sql.NullString
	)
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(cols) > 10 {
		if err := rows.Scan(&actionID, &entityID, &action, &status, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason, &oldValues, &newValues, &changeSummary); err != nil {
			return nil, err
		}
	} else if err := rows.Scan(&actionID, &entityID, &action, &status, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
		return nil, err
	}
	entry := map[string]interface{}{
		"audit_id":          actionID,
		"action_id":         actionID,
		"entity_id":         entityID,
		"action_type":       nullString(action),
		"processing_status": nullString(status),
		"requested_by":      nullString(requestedBy),
		"requested_at":      nullTime(requestedAt),
		"checker_by":        nullString(checkerBy),
		"checker_at":        nullTime(checkerAt),
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

func queryFXDownloadAudit(r *http.Request, db *sql.DB, cfg fxAuditConfig, parentID string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT %s, requested_by, requested_at, file_name, upload_s3_key
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
		var fileName, uploadKey sql.NullString
		if err := rows.Scan(&entityID, &requestedBy, &requestedAt, &fileName, &uploadKey); err != nil {
			return nil, err
		}
		entry := map[string]interface{}{
			"entity_id":         entityID,
			"action_type":       "DOWNLOAD",
			"processing_status": "COMPLETED",
			"requested_by":      strings.TrimSpace(requestedBy),
			"requested_at":      nullTime(requestedAt),
			"checker_by":        "",
			"checker_at":        nil,
			"checker_comment":   "",
			"reason":            "",
			"file_name":         nullString(fileName),
			"upload_s3_key":     nullString(uploadKey),
			"download_type":     cfg.DownloadType,
			"source":            cfg.Source,
		}
		payload = append(payload, entry)
	}
	return payload, rows.Err()
}

func writeFXAuditError(w http.ResponseWriter, status int, message string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		constants.ValueSuccess: false,
		constants.ValueError:   message,
	})
}

func nullString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func nullTime(value sql.NullTime) interface{} {
	if !value.Valid {
		return nil
	}
	return value.Time
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
