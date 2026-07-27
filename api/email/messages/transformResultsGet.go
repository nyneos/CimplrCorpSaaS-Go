package emailmessages

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	emailcommon "CimplrCorpSaas/api/email/common"
	"CimplrCorpSaas/api/utils/s3storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

type transformResultDelivery struct {
	DeliveryID       string `json:"delivery_id"`
	DestinationID    string `json:"destination_id,omitempty"`
	DestinationType  string `json:"destination_type"`
	OutputLocation   string `json:"output_location"`
	OutputFilename   string `json:"output_filename"`
	TransformedS3Key string `json:"transformed_s3_key"`
	Status           string `json:"status"`
	ErrorMessage     string `json:"error_message,omitempty"`
}

func HandleTransformResultGet(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}

		var req struct {
			ResultID string `json:"result_id"`
			UserID   string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "Invalid request body")
			return
		}
		if req.ResultID == "" {
			emailcommon.RespondBadRequest(w, "result_id is required")
			return
		}

		_, _, _, _ = emailcommon.RequestIdentity(r, "", "")

		query := `
			SELECT 
			    tr.result_id::text, 
			    tr.attachment_id::text, 
			    tr.rule_id::text, 
			    COALESCE(tr.transformed_s3_key, ''),
			    COALESCE(NULLIF(tr.destination_type, ''), 'S3'),
			    COALESCE(tr.output_location, ''),
			    COALESCE(tr.output_filename, ''),
			    tr.created_at,
			    m.message_id::text, 
			    COALESCE(m.inbox_id::text, ''), 
			    COALESCE(m.envelope_from, ''),
			    COALESCE(m.subject, ''),
			    COALESCE(m.received_at, m.created_at) as message_date,
			    ma.filename,
			    ma.s3_key as original_s3_key,
			    COALESCE(r.rule_name, ''),
			    COALESCE(r.mapping_name, '')
			FROM email_svc.transformation_results tr
			JOIN email_svc.message_attachment ma ON ma.attachment_id = tr.attachment_id
			JOIN email_svc.message m ON m.message_id = ma.message_id
			LEFT JOIN email_svc.transformation_rules r ON r.rule_id = tr.rule_id
			WHERE tr.result_id = $1::uuid
			LIMIT 1
		`

		var res struct {
			ResultID               string                    `json:"id"`
			AttachmentID           string                    `json:"attachment_id"`
			RuleID                 string                    `json:"rule_id"`
			TransformedS3Key       string                    `json:"transformed_s3_key"`
			DestinationType        string                    `json:"destination_type"`
			OutputLocation         string                    `json:"output_location"`
			OutputFilename         string                    `json:"output_filename"`
			CreatedAt              time.Time                 `json:"created_at"`
			MessageID              string                    `json:"message_id"`
			InboxID                string                    `json:"inbox_id"`
			EnvelopeFrom           string                    `json:"envelope_from"`
			Subject                string                    `json:"subject"`
			MessageDate            time.Time                 `json:"message_date"`
			Filename               string                    `json:"filename"`
			OriginalS3Key          string                    `json:"original_s3_key"`
			RuleName               string                    `json:"rule_name"`
			MappingName            string                    `json:"mapping_name"`
			OriginalDownloadURL    string                    `json:"original_download_url"`
			TransformedDownloadURL string                    `json:"transformed_download_url"`
			LocalDownloadAvailable bool                      `json:"local_download_available"`
			APIPreviewAvailable    bool                      `json:"api_preview_available"`
			Deliveries             []transformResultDelivery `json:"deliveries"`
		}

		err := pool.QueryRow(r.Context(), query, req.ResultID).Scan(
			&res.ResultID,
			&res.AttachmentID,
			&res.RuleID,
			&res.TransformedS3Key,
			&res.DestinationType,
			&res.OutputLocation,
			&res.OutputFilename,
			&res.CreatedAt,
			&res.MessageID,
			&res.InboxID,
			&res.EnvelopeFrom,
			&res.Subject,
			&res.MessageDate,
			&res.Filename,
			&res.OriginalS3Key,
			&res.RuleName,
			&res.MappingName,
		)
		if err != nil {
			emailcommon.RespondInternal(w, "Failed to fetch transformation result: "+err.Error())
			return
		}

		res.Deliveries = []transformResultDelivery{}
		drows, derr := pool.Query(r.Context(), `
			SELECT delivery_id::text,
			       COALESCE(destination_id::text, ''),
			       COALESCE(NULLIF(destination_type, ''), 'S3'),
			       COALESCE(output_location, ''),
			       COALESCE(output_filename, ''),
			       COALESCE(transformed_s3_key, ''),
			       COALESCE(NULLIF(status, ''), 'SUCCESS'),
			       COALESCE(error_message, '')
			FROM email_svc.transformation_result_deliveries
			WHERE result_id = $1::uuid
			ORDER BY created_at ASC
		`, req.ResultID)
		if derr == nil {
			defer drows.Close()
			for drows.Next() {
				var d transformResultDelivery
				if scanErr := drows.Scan(
					&d.DeliveryID, &d.DestinationID, &d.DestinationType,
					&d.OutputLocation, &d.OutputFilename, &d.TransformedS3Key,
					&d.Status, &d.ErrorMessage,
				); scanErr == nil {
					res.Deliveries = append(res.Deliveries, d)
				}
			}
		}
		if len(res.Deliveries) == 0 && (res.OutputLocation != "" || res.TransformedS3Key != "") {
			res.Deliveries = append(res.Deliveries, transformResultDelivery{
				DestinationType:  res.DestinationType,
				OutputLocation:   res.OutputLocation,
				OutputFilename:   res.OutputFilename,
				TransformedS3Key: res.TransformedS3Key,
				Status:           "SUCCESS",
			})
		}

		if res.OriginalS3Key != "" {
			if url, err := s3storage.GetDownloadPresignedURL(r.Context(), res.OriginalS3Key, 15*time.Minute); err == nil {
				res.OriginalDownloadURL = url
			}
		}
		// Prefer any successful S3 delivery key for preview/download (multi-dest mixes).
		s3KeyForPreview := strings.TrimSpace(res.TransformedS3Key)
		s3NameForPreview := strings.TrimSpace(res.OutputFilename)
		for _, d := range res.Deliveries {
			if !strings.EqualFold(d.Status, "SUCCESS") && d.Status != "" {
				continue
			}
			if !strings.EqualFold(d.DestinationType, "S3") {
				continue
			}
			key := strings.TrimSpace(d.TransformedS3Key)
			if key == "" {
				key = strings.TrimSpace(d.OutputLocation)
			}
			if key == "" {
				continue
			}
			s3KeyForPreview = key
			if name := strings.TrimSpace(d.OutputFilename); name != "" {
				s3NameForPreview = name
			}
			break
		}
		if s3KeyForPreview != "" {
			res.TransformedS3Key = s3KeyForPreview
			if s3NameForPreview != "" && strings.TrimSpace(res.OutputFilename) == "" {
				res.OutputFilename = s3NameForPreview
			}
			if url, err := s3storage.GetDownloadPresignedURL(r.Context(), s3KeyForPreview, 15*time.Minute); err == nil {
				res.TransformedDownloadURL = url
			}
		}
		for _, d := range res.Deliveries {
			if strings.EqualFold(d.DestinationType, "LOCAL") && strings.TrimSpace(d.OutputLocation) != "" &&
				(strings.EqualFold(d.Status, "SUCCESS") || d.Status == "") {
				res.LocalDownloadAvailable = true
			}
		}
		if strings.EqualFold(res.DestinationType, "LOCAL") && strings.TrimSpace(res.OutputLocation) != "" {
			res.LocalDownloadAvailable = true
		}
		// API/SFTP alone are store-only — do not advertise API preview for the Preview button.
		// Preview is driven by S3 (transformed_download_url) or LOCAL.

		emailcommon.RespondPayload(w, "GET_TRANSFORM_RESULT", res)
	}
}
