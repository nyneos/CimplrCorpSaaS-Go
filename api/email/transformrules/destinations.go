package transformrules

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RuleDestination is one output target for a transformation rule (1:many child).
type RuleDestination struct {
	DestinationID    string `json:"destination_id,omitempty"`
	RuleID           string `json:"rule_id,omitempty"`
	SortOrder        int    `json:"sort_order"`
	DestinationType  string `json:"destination_type"`
	OutputNamePrefix string `json:"output_name_prefix"`
	AppendDatetime   *bool  `json:"append_datetime"`
	S3Prefix         string `json:"s3_prefix,omitempty"`
	LocalFolder      string `json:"local_folder,omitempty"`
	SftpHost         string `json:"sftp_host,omitempty"`
	SftpPort         int    `json:"sftp_port,omitempty"`
	SftpUser         string `json:"sftp_user,omitempty"`
	SftpPassword     string `json:"sftp_password"`
	SftpFolder       string `json:"sftp_folder,omitempty"`
	APIURL           string `json:"api_url,omitempty"`
	APIAuthToken     string `json:"api_auth_token"`
	IsActive         *bool  `json:"is_active,omitempty"`
	IsDeleted        bool   `json:"is_deleted,omitempty"`
}

func destActive(d *RuleDestination) bool {
	if d.IsActive == nil {
		return true
	}
	return *d.IsActive
}

func normalizeOneDestination(d *RuleDestination) error {
	if d == nil {
		return fmt.Errorf("destination is required")
	}
	tmp := TransformationRule{
		DestinationType:  d.DestinationType,
		OutputNamePrefix: d.OutputNamePrefix,
		AppendDatetime:   d.AppendDatetime,
		S3Prefix:         d.S3Prefix,
		LocalFolder:      d.LocalFolder,
		SftpHost:         d.SftpHost,
		SftpPort:         d.SftpPort,
		SftpUser:         d.SftpUser,
		SftpPassword:     d.SftpPassword,
		SftpFolder:       d.SftpFolder,
		APIURL:           d.APIURL,
		APIAuthToken:     d.APIAuthToken,
	}
	if err := normalizeDestination(&tmp); err != nil {
		return err
	}
	d.DestinationType = tmp.DestinationType
	d.OutputNamePrefix = tmp.OutputNamePrefix
	d.AppendDatetime = boolPtr(appendDatetimeValue(tmp.AppendDatetime))
	d.S3Prefix = tmp.S3Prefix
	d.LocalFolder = tmp.LocalFolder
	d.SftpHost = tmp.SftpHost
	d.SftpPort = tmp.SftpPort
	d.SftpUser = tmp.SftpUser
	d.SftpPassword = tmp.SftpPassword
	d.SftpFolder = tmp.SftpFolder
	d.APIURL = tmp.APIURL
	d.APIAuthToken = tmp.APIAuthToken
	if d.IsActive == nil {
		d.IsActive = boolPtr(true)
	}
	return nil
}

// resolveDestinations returns the destinations list for a request.
// Prefer destinations[]; else build one from legacy flat fields on the rule.
func resolveDestinations(req *TransformationRule) ([]RuleDestination, error) {
	if req == nil {
		return nil, fmt.Errorf("rule is required")
	}
	if len(req.Destinations) > 0 {
		out := make([]RuleDestination, 0, len(req.Destinations))
		for i := range req.Destinations {
			d := req.Destinations[i]
			d.SortOrder = i
			if err := normalizeOneDestination(&d); err != nil {
				return nil, fmt.Errorf("destination[%d]: %w", i, err)
			}
			out = append(out, d)
		}
		return out, nil
	}
	if err := normalizeDestination(req); err != nil {
		return nil, err
	}
	d := RuleDestination{
		SortOrder:        0,
		DestinationType:  req.DestinationType,
		OutputNamePrefix: req.OutputNamePrefix,
		AppendDatetime:   req.AppendDatetime,
		S3Prefix:         req.S3Prefix,
		LocalFolder:      req.LocalFolder,
		SftpHost:         req.SftpHost,
		SftpPort:         req.SftpPort,
		SftpUser:         req.SftpUser,
		SftpPassword:     req.SftpPassword,
		SftpFolder:       req.SftpFolder,
		APIURL:           req.APIURL,
		APIAuthToken:     req.APIAuthToken,
		IsActive:         boolPtr(true),
	}
	if err := normalizeOneDestination(&d); err != nil {
		return nil, err
	}
	return []RuleDestination{d}, nil
}

// applyPrimaryMirror copies destinations[0] onto the rule's legacy fat columns.
func applyPrimaryMirror(req *TransformationRule, dests []RuleDestination) {
	if len(dests) == 0 {
		return
	}
	p := dests[0]
	req.DestinationType = p.DestinationType
	req.OutputNamePrefix = p.OutputNamePrefix
	req.AppendDatetime = p.AppendDatetime
	req.S3Prefix = p.S3Prefix
	req.LocalFolder = p.LocalFolder
	req.SftpHost = p.SftpHost
	req.SftpPort = p.SftpPort
	req.SftpUser = p.SftpUser
	req.SftpPassword = p.SftpPassword
	req.SftpFolder = p.SftpFolder
	req.APIURL = p.APIURL
	req.APIAuthToken = p.APIAuthToken
	req.Destinations = dests
}

func listDestinations(ctx context.Context, pool *pgxpool.Pool, ruleID string) ([]RuleDestination, error) {
	rows, err := pool.Query(ctx, `
		SELECT destination_id::text, rule_id::text, sort_order,
		       COALESCE(NULLIF(destination_type, ''), 'S3'),
		       COALESCE(output_name_prefix, ''),
		       COALESCE(append_datetime, true),
		       COALESCE(s3_prefix, ''),
		       COALESCE(local_folder, ''),
		       COALESCE(sftp_host, ''),
		       COALESCE(sftp_port, 22),
		       COALESCE(sftp_user, ''),
		       COALESCE(sftp_password, ''),
		       COALESCE(sftp_folder, ''),
		       COALESCE(api_url, ''),
		       COALESCE(api_auth_token, ''),
		       COALESCE(is_active, true),
		       COALESCE(is_deleted, false)
		FROM email_svc.transformation_rule_destinations
		WHERE rule_id = $1::uuid AND is_deleted = false
		ORDER BY sort_order ASC, created_at ASC
	`, ruleID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []RuleDestination
	for rows.Next() {
		var d RuleDestination
		var appendDT, isActive bool
		if err := rows.Scan(
			&d.DestinationID, &d.RuleID, &d.SortOrder,
			&d.DestinationType, &d.OutputNamePrefix, &appendDT,
			&d.S3Prefix, &d.LocalFolder,
			&d.SftpHost, &d.SftpPort, &d.SftpUser, &d.SftpPassword, &d.SftpFolder,
			&d.APIURL, &d.APIAuthToken,
			&isActive, &d.IsDeleted,
		); err != nil {
			return nil, err
		}
		d.AppendDatetime = boolPtr(appendDT)
		d.IsActive = boolPtr(isActive)
		out = append(out, d)
	}
	if out == nil {
		out = []RuleDestination{}
	}
	return out, nil
}

func attachDestinations(ctx context.Context, pool *pgxpool.Pool, rules []TransformationRule) {
	for i := range rules {
		dests, err := listDestinations(ctx, pool, rules[i].RuleID)
		if err != nil || len(dests) == 0 {
			// Fallback: synthesize from fat columns (pre-migration / empty).
			d := RuleDestination{
				SortOrder:        0,
				DestinationType:  rules[i].DestinationType,
				OutputNamePrefix: rules[i].OutputNamePrefix,
				AppendDatetime:   rules[i].AppendDatetime,
				S3Prefix:         rules[i].S3Prefix,
				LocalFolder:      rules[i].LocalFolder,
				SftpHost:         rules[i].SftpHost,
				SftpPort:         rules[i].SftpPort,
				SftpUser:         rules[i].SftpUser,
				SftpPassword:     rules[i].SftpPassword,
				SftpFolder:       rules[i].SftpFolder,
				APIURL:           rules[i].APIURL,
				APIAuthToken:     rules[i].APIAuthToken,
				IsActive:         boolPtr(true),
			}
			rules[i].Destinations = []RuleDestination{d}
			continue
		}
		rules[i].Destinations = dests
		applyPrimaryMirror(&rules[i], dests)
	}
}

func insertDestinationAudit(
	ctx context.Context,
	tx pgx.Tx,
	action, actorID string,
	oldD *RuleDestination,
	newD *RuleDestination,
) error {
	var (
		destID, ruleID string
		oldSort, newSort                                               *int
		oldType, newType, oldPrefix, newPrefix                         *string
		oldAppend, newAppend                                           *bool
		oldS3, newS3, oldLocal, newLocal                               *string
		oldHost, newHost, oldUser, newUser, oldFolder, newFolder       *string
		oldPort, newPort                                               *int
		oldAPI, newAPI                                                 *string
		oldActive, newActive, oldDel, newDel                           *bool
	)
	if newD != nil {
		destID = newD.DestinationID
		ruleID = newD.RuleID
		so := newD.SortOrder
		newSort = &so
		nt := newD.DestinationType
		newType = &nt
		np := newD.OutputNamePrefix
		newPrefix = &np
		na := appendDatetimeValue(newD.AppendDatetime)
		newAppend = &na
		ns3 := newD.S3Prefix
		newS3 = &ns3
		nl := newD.LocalFolder
		newLocal = &nl
		nh := newD.SftpHost
		newHost = &nh
		nport := newD.SftpPort
		newPort = &nport
		nu := newD.SftpUser
		newUser = &nu
		nf := newD.SftpFolder
		newFolder = &nf
		napi := newD.APIURL
		newAPI = &napi
		nact := destActive(newD)
		newActive = &nact
		nd := newD.IsDeleted
		newDel = &nd
	}
	if oldD != nil {
		if destID == "" {
			destID = oldD.DestinationID
		}
		if ruleID == "" {
			ruleID = oldD.RuleID
		}
		so := oldD.SortOrder
		oldSort = &so
		ot := oldD.DestinationType
		oldType = &ot
		op := oldD.OutputNamePrefix
		oldPrefix = &op
		oa := appendDatetimeValue(oldD.AppendDatetime)
		oldAppend = &oa
		os3 := oldD.S3Prefix
		oldS3 = &os3
		ol := oldD.LocalFolder
		oldLocal = &ol
		oh := oldD.SftpHost
		oldHost = &oh
		oport := oldD.SftpPort
		oldPort = &oport
		ou := oldD.SftpUser
		oldUser = &ou
		of := oldD.SftpFolder
		oldFolder = &of
		oapi := oldD.APIURL
		oldAPI = &oapi
		oact := destActive(oldD)
		oldActive = &oact
		od := oldD.IsDeleted
		oldDel = &od
	}
	if ruleID == "" {
		return fmt.Errorf("rule_id required for destination audit")
	}
	var destIDArg interface{}
	if destID != "" {
		destIDArg = destID
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO email_svc.transformation_rule_destinations_audit (
			destination_id, rule_id, action_type, actor_id,
			old_sort_order, new_sort_order,
			old_destination_type, new_destination_type,
			old_output_name_prefix, new_output_name_prefix,
			old_append_datetime, new_append_datetime,
			old_s3_prefix, new_s3_prefix,
			old_local_folder, new_local_folder,
			old_sftp_host, new_sftp_host,
			old_sftp_port, new_sftp_port,
			old_sftp_user, new_sftp_user,
			old_sftp_folder, new_sftp_folder,
			old_api_url, new_api_url,
			old_is_active, new_is_active,
			old_is_deleted, new_is_deleted
		) VALUES (
			$1::uuid, $2::uuid, $3, $4,
			$5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
			$17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30
		)
	`, destIDArg, ruleID, action, actorID,
		oldSort, newSort, oldType, newType, oldPrefix, newPrefix,
		oldAppend, newAppend, oldS3, newS3, oldLocal, newLocal,
		oldHost, newHost, oldPort, newPort, oldUser, newUser, oldFolder, newFolder,
		oldAPI, newAPI, oldActive, newActive, oldDel, newDel,
	)
	return err
}

// replaceDestinations soft-deletes existing rows and inserts the new set.
// Also writes destination audit rows with old_/new_ pairs.
func replaceDestinations(
	ctx context.Context,
	pool *pgxpool.Pool,
	ruleID, actorID string,
	dests []RuleDestination,
) ([]RuleDestination, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	oldRows, err := listDestinations(ctx, pool, ruleID)
	if err != nil {
		// Table may not exist yet during rolling deploys — ignore empty.
		oldRows = nil
	}

	// Preserve secrets when client sends empty password/token for an existing row.
	oldByID := map[string]RuleDestination{}
	oldBySort := map[int]RuleDestination{}
	for _, o := range oldRows {
		oldByID[o.DestinationID] = o
		oldBySort[o.SortOrder] = o
	}
	for i := range dests {
		prev, ok := oldByID[dests[i].DestinationID]
		if !ok {
			prev, ok = oldBySort[dests[i].SortOrder]
		}
		if !ok {
			continue
		}
		if strings.TrimSpace(dests[i].SftpPassword) == "" {
			dests[i].SftpPassword = prev.SftpPassword
		}
		if strings.TrimSpace(dests[i].APIAuthToken) == "" {
			dests[i].APIAuthToken = prev.APIAuthToken
		}
	}

	for i := range oldRows {
		old := oldRows[i]
		old.IsDeleted = true
		if err := insertDestinationAudit(ctx, tx, "DELETE", actorID, &oldRows[i], &old); err != nil {
			return nil, err
		}
	}

	_, err = tx.Exec(ctx, `
		UPDATE email_svc.transformation_rule_destinations
		SET is_deleted = true, updated_at = now()
		WHERE rule_id = $1::uuid AND is_deleted = false
	`, ruleID)
	if err != nil {
		return nil, err
	}

	inserted := make([]RuleDestination, 0, len(dests))
	for i := range dests {
		d := dests[i]
		d.RuleID = ruleID
		d.SortOrder = i
		d.IsDeleted = false
		if d.IsActive == nil {
			d.IsActive = boolPtr(true)
		}
		appendDT := appendDatetimeValue(d.AppendDatetime)
		err := tx.QueryRow(ctx, `
			INSERT INTO email_svc.transformation_rule_destinations (
				rule_id, sort_order, destination_type, output_name_prefix, append_datetime,
				s3_prefix, local_folder,
				sftp_host, sftp_port, sftp_user, sftp_password, sftp_folder,
				api_url, api_auth_token, is_active, is_deleted
			) VALUES (
				$1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, false
			)
			RETURNING destination_id::text
		`, ruleID, d.SortOrder, d.DestinationType, d.OutputNamePrefix, appendDT,
			d.S3Prefix, d.LocalFolder,
			d.SftpHost, d.SftpPort, d.SftpUser, d.SftpPassword, d.SftpFolder,
			d.APIURL, d.APIAuthToken, destActive(&d),
		).Scan(&d.DestinationID)
		if err != nil {
			return nil, err
		}
		d.AppendDatetime = boolPtr(appendDT)
		action := "CREATE"
		var oldPtr *RuleDestination
		if i < len(oldRows) {
			action = "UPDATE"
			oldPtr = &oldRows[i]
		}
		if err := insertDestinationAudit(ctx, tx, action, actorID, oldPtr, &d); err != nil {
			return nil, err
		}
		inserted = append(inserted, d)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return inserted, nil
}
