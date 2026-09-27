package mailruntime

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/mailengine/model"
	"CimplrCorpSaas/internal/mailengine/storage"
)

func (r *Runtime) PutStorage(ctx context.Context, req StoragePutRequest) (*StoragePutResult, error) {
	var out StoragePutResult
	err := runIsolated(ctx, 0, func(ctx context.Context) error {
		if strings.TrimSpace(req.ContentBase64) == "" {
			return fmt.Errorf("content_base64 is required")
		}
		dt := req.DestinationType
		if strings.TrimSpace(dt) == "" {
			dt = storage.DestS3
		}
		logger.LogInfoCtx(ctx, "[mail] storage/put: destination=%s prefix=%q", dt, req.OutputNamePrefix)
		result, err := storage.Put(ctx, model.StoragePutRequest{
			ContentBase64:    req.ContentBase64,
			ContentType:      req.ContentType,
			FileExt:          req.FileExt,
			DestinationType:  dt,
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
		})
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] storage/put: failed dest=%s err=%v", dt, err)
			return err
		}
		out = StoragePutResult{
			DestinationType: result.DestinationType,
			OutputFilename:  result.OutputFilename,
			OutputLocation:  result.OutputLocation,
			S3Key:           result.S3Key,
		}
		logger.LogInfoCtx(ctx, "[mail] storage/put: ok dest=%s file=%s location=%s", out.DestinationType, out.OutputFilename, out.OutputLocation)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// ReadAPIInbox reads back a transformed file from the demo API-inbox folders
// (used by the API-destination test-receive endpoints during onboarding).
func (r *Runtime) ReadAPIInbox(ctx context.Context, req ReadAPIInboxRequest) (*ReadAPIInboxResult, error) {
	var out ReadAPIInboxResult
	err := runIsolated(ctx, 0, func(ctx context.Context) error {
		name := filepath.Base(strings.TrimSpace(req.Filename))
		if name == "" || name == "." {
			return fmt.Errorf("filename is required")
		}
		subfolder := strings.TrimSpace(req.Folder)
		if subfolder == "" {
			subfolder = "api-inbox"
		}
		if subfolder != "api-inbox" && subfolder != "api-inbox-2" {
			return fmt.Errorf("folder must be api-inbox or api-inbox-2")
		}

		base := strings.TrimSpace(os.Getenv("EMAIL_TRANSFORMED_LOCAL_DIR"))
		if base == "" {
			base = "./transformed"
		}
		full := filepath.Join(base, subfolder, name)
		abs, err := filepath.Abs(full)
		if err != nil {
			return err
		}
		baseAbs, err := filepath.Abs(base)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(baseAbs, abs)
		if err != nil || strings.HasPrefix(rel, "..") {
			return fmt.Errorf("invalid path")
		}

		raw, err := os.ReadFile(abs)
		if err != nil {
			return fmt.Errorf("file not found: %w", err)
		}
		logger.LogInfoCtx(ctx, "[mail] storage/read-api-inbox: %s (%d bytes)", abs, len(raw))
		out = ReadAPIInboxResult{
			Filename:      name,
			Folder:        subfolder,
			Path:          abs,
			ContentBase64: base64.StdEncoding.EncodeToString(raw),
			ByteSize:      len(raw),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}
