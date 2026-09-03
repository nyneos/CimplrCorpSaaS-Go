package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DashboardSummary returns KPI + chart payloads for the FD rate negotiation dashboard.
func DashboardSummary(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := requireSessionEmail(w, r); !ok {
			return
		}
		ctx := r.Context()

		out := map[string]interface{}{
			"totals":                map[string]interface{}{},
			"by_status":             []map[string]interface{}{},
			"by_bank":               []map[string]interface{}{},
			"recent_requests":       []map[string]interface{}{},
			"recent_communications": []map[string]interface{}{},
			"recent_offers":         []map[string]interface{}{},
			"inbound_emails":        []map[string]interface{}{},
		}

		var total, pending, approved, sent, offers, converted int
		var totalAmount float64
		_ = pool.QueryRow(ctx, `
			SELECT
				COUNT(*)::int,
				COUNT(*) FILTER (WHERE request_status IN (
					'PENDING_APPROVAL','PENDING_EDIT_APPROVAL','PENDING_DELETE_APPROVAL','PENDING_RATE_APPROVAL'
				))::int,
				COUNT(*) FILTER (WHERE request_status = 'APPROVED')::int,
				COUNT(*) FILTER (WHERE request_status IN ('SENT_TO_BANKS','RESPONSE_RECEIVED','OFFERS_RECEIVED'))::int,
				COUNT(*) FILTER (WHERE request_status = 'CONVERTED_TO_FD')::int,
				COALESCE(SUM(proposed_fd_amount),0)
			FROM investment.fd_rate_negotiation
			WHERE COALESCE(is_deleted,false)=false`).Scan(
			&total, &pending, &approved, &sent, &converted, &totalAmount,
		)
		_ = pool.QueryRow(ctx, `
			SELECT COUNT(*)::int
			FROM investment.fd_rate_offer
			WHERE COALESCE(is_deleted,false)=false`).Scan(&offers)

		out["totals"] = map[string]interface{}{
			"requests":        total,
			"pending_checker": pending,
			"approved":        approved,
			"in_market":       sent,
			"offers":          offers,
			"converted_to_fd": converted,
			"proposed_amount": totalAmount,
		}

		statusRows, err := pool.Query(ctx, `
			SELECT COALESCE(request_status,'UNKNOWN'), COUNT(*)::int, COALESCE(SUM(proposed_fd_amount),0)
			FROM investment.fd_rate_negotiation
			WHERE COALESCE(is_deleted,false)=false
			GROUP BY 1
			ORDER BY 2 DESC`)
		if err == nil {
			defer statusRows.Close()
			items := make([]map[string]interface{}, 0)
			for statusRows.Next() {
				var status string
				var count int
				var amount float64
				if err := statusRows.Scan(&status, &count, &amount); err != nil {
					continue
				}
				items = append(items, map[string]interface{}{
					"status": status, "count": count, "amount": amount,
				})
			}
			out["by_status"] = items
		}

		bankRows, err := pool.Query(ctx, `
			SELECT COALESCE(NULLIF(b,''),'Unassigned'), COUNT(*)::int, COALESCE(SUM(m.proposed_fd_amount),0)
			FROM investment.fd_rate_negotiation m
			LEFT JOIN LATERAL unnest(COALESCE(m.target_bank_names, ARRAY[]::text[])) AS b ON true
			WHERE COALESCE(m.is_deleted,false)=false
			GROUP BY 1
			ORDER BY 2 DESC
			LIMIT 12`)
		if err == nil {
			defer bankRows.Close()
			items := make([]map[string]interface{}, 0)
			for bankRows.Next() {
				var bank string
				var count int
				var amount float64
				if err := bankRows.Scan(&bank, &count, &amount); err != nil {
					continue
				}
				items = append(items, map[string]interface{}{
					"bank": bank, "count": count, "amount": amount,
				})
			}
			out["by_bank"] = items
		}

		reqRows, err := pool.Query(ctx, `
			SELECT
				rate_request_id::text,
				COALESCE(rate_request_ref,''),
				COALESCE(request_status,''),
				COALESCE(proposed_fd_amount,0),
				COALESCE(currency_code,''),
				COALESCE(array_to_string(target_bank_names, ', '),''),
				COALESCE(created_by,''),
				COALESCE(TO_CHAR(created_at AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI'),'')
			FROM investment.fd_rate_negotiation
			WHERE COALESCE(is_deleted,false)=false
			  AND request_status NOT IN ('REJECTED','CANCELLED')
			ORDER BY created_at DESC, rate_request_id DESC
			LIMIT 12`)
		if err == nil {
			defer reqRows.Close()
			items := make([]map[string]interface{}, 0)
			for reqRows.Next() {
				var id, ref, status, currency, banks, createdBy, createdAt string
				var amount float64
				if err := reqRows.Scan(&id, &ref, &status, &amount, &currency, &banks, &createdBy, &createdAt); err != nil {
					continue
				}
				items = append(items, map[string]interface{}{
					"rate_request_id":    id,
					"rate_request_ref":   ref,
					"request_status":     status,
					"proposed_fd_amount": amount,
					"currency_code":      currency,
					"target_banks":       banks,
					"created_by":         createdBy,
					"created_at":         createdAt,
				})
			}
			out["recent_requests"] = items
		}

		commRows, err := pool.Query(ctx, `
			SELECT
				c.communication_id::text,
				COALESCE(m.rate_request_ref,''),
				COALESCE(c.communication_mode,''),
				COALESCE(c.communication_status,''),
				COALESCE(c.email_template_name,''),
				COALESCE(c.sent_by, c.created_by,''),
				COALESCE(TO_CHAR(COALESCE(c.sent_at, c.created_at) AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI'),'')
			FROM investment.fd_rate_communication c
			JOIN investment.fd_rate_negotiation m ON m.rate_request_id = c.rate_request_id
			WHERE COALESCE(c.is_deleted,false)=false
			  AND c.communication_status <> 'REJECTED'
			ORDER BY COALESCE(c.sent_at, c.created_at) DESC, c.communication_id DESC`)
		if err == nil {
			defer commRows.Close()
			items := make([]map[string]interface{}, 0)
			for commRows.Next() {
				var id, ref, mode, status, tpl, actor, at string
				if err := commRows.Scan(&id, &ref, &mode, &status, &tpl, &actor, &at); err != nil {
					continue
				}
				items = append(items, map[string]interface{}{
					"communication_id":     id,
					"rate_request_ref":     ref,
					"communication_mode":   mode,
					"communication_status": status,
					"email_template_name":  tpl,
					"actor":                actor,
					"at":                   at,
				})
			}
			out["recent_communications"] = items
		}

		offerRows, err := pool.Query(ctx, `
			SELECT
				o.offer_id::text,
				o.rate_request_id::text,
				COALESCE(m.rate_request_ref,''),
				COALESCE(o.bank_name,''),
				COALESCE(o.offered_interest_rate,0),
				COALESCE(o.offer_status,''),
				COALESCE(TO_CHAR(o.valid_till_date,'YYYY-MM-DD'),'')
			FROM investment.fd_rate_offer o
			JOIN investment.fd_rate_negotiation m ON m.rate_request_id = o.rate_request_id
			WHERE COALESCE(o.is_deleted,false)=false
			  AND o.offer_status NOT IN ('REJECTED','EXPIRED')
			  AND (o.valid_till_date IS NULL OR o.valid_till_date >= CURRENT_DATE)
			ORDER BY o.created_at DESC, o.offer_id DESC`)
		if err == nil {
			defer offerRows.Close()
			items := make([]map[string]interface{}, 0)
			for offerRows.Next() {
				var id, requestID, ref, bank, status, valid string
				var rate float64
				if err := offerRows.Scan(&id, &requestID, &ref, &bank, &rate, &status, &valid); err != nil {
					continue
				}
				items = append(items, map[string]interface{}{
					"offer_id":              id,
					"rate_request_id":       requestID,
					"rate_request_ref":      ref,
					"bank_name":             bank,
					"offered_interest_rate": rate,
					"offer_status":          status,
					"valid_till_date":       valid,
				})
			}
			out["recent_offers"] = items
		}

		mailRows, err := pool.Query(ctx, `
			SELECT
				message_id::text,
				COALESCE(envelope_from,''),
				COALESCE(subject,''),
				COALESCE(entity_id,''),
				COALESCE(processing_status,''),
				COALESCE(body_text_preview,''),
				COALESCE(TO_CHAR(COALESCE(received_at, created_at) AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI'),'')
			FROM email_svc.message
			WHERE module = 'fd-rate-negotiation'
			ORDER BY COALESCE(received_at, created_at) DESC, message_id DESC
			LIMIT 12`)
		if err == nil {
			defer mailRows.Close()
			items := make([]map[string]interface{}, 0)
			for mailRows.Next() {
				var id, from, subject, entity, status, preview, at string
				if err := mailRows.Scan(&id, &from, &subject, &entity, &status, &preview, &at); err != nil {
					continue
				}
				items = append(items, map[string]interface{}{
					"message_id":        id,
					"envelope_from":     from,
					"subject":           subject,
					"entity_id":         entity,
					"processing_status": status,
					"body_text_preview": preview,
					"received_at":       at,
				})
			}
			out["inbound_emails"] = items
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// ListCommunicationTemplates returns approved DMS EMAIL templates for FD rate
// negotiation bank send. Source is dms_svc (doc-service), never notification_svc.
func ListCommunicationTemplates(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := requireSessionEmail(w, r); !ok {
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&struct{}{})
		ctx := r.Context()

		rows, err := pool.Query(ctx, `
			SELECT
				t.template_id::text,
				t.name,
				COALESCE('v' || tv.version_no::text, 'v1'),
				COALESCE(tv.content_json->>'html', ''),
				COALESCE(tv.content_json->'emailMeta'->>'subject', ''),
				(t.status = 'Active')
			FROM dms_svc.template t
			JOIN LATERAL (
				SELECT v.version_id, v.version_no, v.content_json
				FROM dms_svc.template_version v
				WHERE v.template_id = t.template_id
				  AND COALESCE(v.is_deleted, false) = false
				  AND (
				    t.current_version_id IS NOT NULL AND v.version_id = t.current_version_id
				    OR t.current_version_id IS NULL AND v.status = 'APPROVED'
				  )
				ORDER BY CASE WHEN v.version_id = t.current_version_id THEN 0 ELSE 1 END, v.version_no DESC
				LIMIT 1
			) tv ON true
			WHERE t.is_deleted = false
			  AND t.module_code = 'INVESTMENT_FD'
			  AND t.sub_module_code = 'FD_RATE_NEGOTIATION'
			  AND t.status = 'Active'
			  AND UPPER(COALESCE(t.processing_status, '')) = 'APPROVED'
			  AND UPPER(COALESCE(tv.content_json->>'kind', '')) = 'EMAIL'
			ORDER BY t.name`)
		if err != nil {
			api.LogErrorForResponse(w, "fd rate negotiation dms templates: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to list email templates")
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var id, name, version, body, subject string
			var active bool
			if err := rows.Scan(&id, &name, &version, &body, &subject, &active); err != nil {
				continue
			}
			out = append(out, map[string]interface{}{
				"template_id":      id,
				"template_name":    name,
				"template_version": version,
				"content":          body,
				"subject":          subject,
				"is_active":        active,
				"source":           "DMS",
			})
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"results": out})
	}
}
