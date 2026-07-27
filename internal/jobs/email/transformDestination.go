package emailjobs

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"CimplrCorpSaas/internal/services/mailruntime"
)

// ruleDestination carries storage + naming config for one destination row.
// S3 / SFTP / API → CIMPLR-Email-Service POST /v1/storage/put.
// LOCAL → written on this Cimplr Go host (per-user folder under CIMPLR_TRANSFORMED_LOCAL_DIR).
type ruleDestination struct {
	DestinationID    string
	DestinationType  string
	OutputNamePrefix string
	AppendDatetime   bool
	S3Prefix         string
	LocalFolder      string
	SftpHost         string
	SftpPort         int
	SftpUser         string
	SftpPassword     string
	SftpFolder       string
	APIURL           string
	APIAuthToken     string
	CreatedBy        string
	ApprovedBy       string
}

// deliverTransformed persists transformed bytes to the rule destination.
func deliverTransformed(
	ctx context.Context,
	dest ruleDestination,
	fileExt string,
	body []byte,
	contentType string,
) (location, s3Key, filename string, err error) {
	dt := strings.ToUpper(strings.TrimSpace(dest.DestinationType))
	if dt == "" {
		dt = "S3"
	}

	// LOCAL is always on the Cimplr Go machine (not email-service).
	if dt == "LOCAL" {
		abs, name, werr := putLocalOnCimplr(dest, fileExt, body)
		if werr != nil {
			return "", "", "", werr
		}
		return abs, "", name, nil
	}

	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		return "", "", "", fmt.Errorf("email service not configured (EMAIL_SERVICE_URL / EMAIL_SERVICE_KEY)")
	}
	out, err := rt.PutStorage(ctx, mailruntime.StoragePutRequest{
		ContentBase64:    base64.StdEncoding.EncodeToString(body),
		ContentType:      contentType,
		FileExt:          fileExt,
		DestinationType:  dt,
		OutputNamePrefix: dest.OutputNamePrefix,
		AppendDatetime:   dest.AppendDatetime,
		S3Prefix:         dest.S3Prefix,
		LocalFolder:      dest.LocalFolder,
		SftpHost:         dest.SftpHost,
		SftpPort:         dest.SftpPort,
		SftpUser:         dest.SftpUser,
		SftpPassword:     dest.SftpPassword,
		SftpFolder:       dest.SftpFolder,
		APIURL:           dest.APIURL,
		APIAuthToken:     dest.APIAuthToken,
	})
	if err != nil {
		return "", "", "", err
	}
	if out == nil {
		return "", "", "", fmt.Errorf("email service returned empty storage result")
	}
	return out.OutputLocation, out.S3Key, out.OutputFilename, nil
}
