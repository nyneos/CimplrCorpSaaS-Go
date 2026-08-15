package dmsjobs

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/services/docsvc"

	"github.com/jackc/pgx/v5/pgxpool"
)

// renderedFile is one generated attachment payload ready for S3.
type renderedFile struct {
	Bytes       []byte
	Ext         string // including leading dot, e.g. ".pdf"
	ContentType string
	Format      string // HTML | PDF | DOCX | XLSX
	// StoredKey is set when the render call also stored the object
	// (renderAndStoreViaDocSvc) — storage_backend='DOCSVC_S3' when non-empty.
	// Empty means the caller must still upload itself (storage_backend='MAIN_S3').
	StoredKey string
}

func renderMergedOutputViaDocSvc(
	ctx context.Context,
	format, mergedHTML string,
	mergeValues map[string]string,
	sheetTokens []string,
	sheetRows [][]string,
	kind string,
	pageDesign pageDesignJSON,
) (renderedFile, error) {
	format = strings.ToUpper(strings.TrimSpace(format))
	data, err := docsvc.NewFromEnv().RenderFormat(ctx, docsvc.RenderFormatRequest{
		OutputFormat: format,
		MergedHTML:   mergedHTML,
		MergeValues:  mergeValues,
		SheetTokens:  sheetTokens,
		SheetRows:    sheetRows,
		Kind:         kind,
		PageDesign:   toDocSvcPageDesign(pageDesign),
	})
	if err != nil {
		return renderedFile{}, fmt.Errorf("document-service render: %w", err)
	}
	raw, err := base64.StdEncoding.DecodeString(data.BytesBase64)
	if err != nil {
		return renderedFile{}, fmt.Errorf("document-service render decode: %w", err)
	}
	return renderedFile{
		Bytes:       raw,
		Ext:         data.Ext,
		ContentType: data.ContentType,
		Format:      data.Format,
	}, nil
}

// renderAndStoreViaDocSvc is renderMergedOutputViaDocSvc's sibling that also
// has Document-Service upload the result to its own S3 bucket, returning the
// key in renderedFile.StoredKey. Callers still get bytes back (needed for the
// LOCAL destination case and checksum verification) — they just skip their
// own S3 upload when StoredKey is non-empty. On any error from the store
// endpoint (including "not configured"), falls back to the plain render call
// so a Document-Service deploy without S3 configured yet doesn't break
// generation — it just keeps using storage_backend='MAIN_S3' as before.
func renderAndStoreViaDocSvc(
	ctx context.Context,
	format, mergedHTML string,
	mergeValues map[string]string,
	sheetTokens []string,
	sheetRows [][]string,
	kind string,
	pageDesign pageDesignJSON,
) (renderedFile, error) {
	format = strings.ToUpper(strings.TrimSpace(format))
	data, err := docsvc.NewFromEnv().RenderAndStore(ctx, docsvc.RenderFormatRequest{
		OutputFormat: format,
		MergedHTML:   mergedHTML,
		MergeValues:  mergeValues,
		SheetTokens:  sheetTokens,
		SheetRows:    sheetRows,
		Kind:         kind,
		PageDesign:   toDocSvcPageDesign(pageDesign),
	})
	if err != nil {
		api.LogInfo("[DMS] render+store unavailable, falling back to render-only (MAIN_S3): %v", err)
		return renderMergedOutputViaDocSvc(ctx, format, mergedHTML, mergeValues, sheetTokens, sheetRows, kind, pageDesign)
	}
	raw, err := base64.StdEncoding.DecodeString(data.BytesBase64)
	if err != nil {
		return renderedFile{}, fmt.Errorf("document-service render+store decode: %w", err)
	}
	return renderedFile{
		Bytes:       raw,
		Ext:         data.Ext,
		ContentType: data.ContentType,
		Format:      data.Format,
		StoredKey:   data.Key,
	}, nil
}

// DownloadURLViaDocSvc returns a short-lived presigned link for a
// DOCSVC_S3-backed document — exported for api/dms/rules' Sent Box download
// handler, which lives outside this package.
func DownloadURLViaDocSvc(ctx context.Context, key, contentType string, inline bool) (string, error) {
	return docsvc.NewFromEnv().DownloadDocumentURL(ctx, key, contentType, inline)
}

func toDocSvcPageDesign(pd pageDesignJSON) docsvc.PageDesignJSON {
	return docsvc.PageDesignJSON{
		HeaderText:        pd.HeaderText,
		FooterText:        pd.FooterText,
		WatermarkText:     pd.WatermarkText,
		WatermarkDataURL:  pd.WatermarkDataURL,
		WatermarkAngle:    pd.WatermarkAngle,
		LetterheadDataURL: pd.LetterheadDataURL,
		LogoDataURL:       pd.LogoDataURL,
		FooterLogoDataURL: pd.FooterLogoDataURL,
		BackgroundColor:   pd.BackgroundColor,
		LogoAlign:         pd.LogoAlign,
		HeaderAlign:       pd.HeaderAlign,
		PageSize:          pd.PageSize,
		Orientation:       pd.Orientation,
		MarginTop:         pd.MarginTop,
		MarginRight:       pd.MarginRight,
		MarginBottom:      pd.MarginBottom,
		MarginLeft:        pd.MarginLeft,
	}
}

func incrementDocSvcQuotaForRun(ctx context.Context, pool *pgxpool.Pool, runID, actor string) {
	if pool == nil || strings.TrimSpace(runID) == "" {
		return
	}
	var byteCount, docCount int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(file_size),0), COUNT(*)
		FROM dms_svc.generated_document WHERE run_id = $1::uuid`, runID,
	).Scan(&byteCount, &docCount); err != nil {
		api.LogError("[DMS] quota increment scan run=%s: %v", runID, err)
		return
	}
	if docCount <= 0 {
		return
	}
	if err := docsvc.NewFromEnv().QuotaIncrement(ctx, docCount, byteCount, actor); err != nil {
		api.LogError("[DMS] document-service quota increment run=%s bytes=%d docs=%d: %v", runID, byteCount, docCount, err)
	}
}
