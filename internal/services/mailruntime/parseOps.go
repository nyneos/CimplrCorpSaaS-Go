package mailruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/mailengine/extract"
	"CimplrCorpSaas/internal/mailengine/model"
	"CimplrCorpSaas/internal/mailengine/parser"
	"CimplrCorpSaas/internal/mailengine/s3store"
)

func (r *Runtime) ListPendingKeys(ctx context.Context, after string, limit int) ([]string, error) {
	var out PendingKeysResult
	err := runIsolated(ctx, 0, func(ctx context.Context) error {
		l := int32(limit)
		if l <= 0 {
			l = 10000
		}
		keys, err := s3store.ListNewRawKeys(ctx, strings.TrimSpace(after), l)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] list-new: failed after=%q err=%v", after, err)
			return err
		}
		out.Prefix = s3store.RawPrefix()
		out.Keys = keys
		logger.LogInfoCtx(ctx, "[mail] list-new: after=%q found=%d prefix=%s", after, len(keys), out.Prefix)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out.Keys, nil
}

func (r *Runtime) DecodeMessages(ctx context.Context, keys []string) (*BatchDecodeResult, error) {
	var out BatchDecodeResult
	err := runIsolated(ctx, 0, func(ctx context.Context) error {
		logger.LogInfoCtx(ctx, "[mail] parse/batch: start count=%d", len(keys))
		for _, key := range keys {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			parsed, err := parser.ParseFromS3(ctx, key)
			if err != nil {
				logger.LogErrorCtx(ctx, "[mail] parse/batch: failed key=%s err=%v", key, err)
				out.Errors = append(out.Errors, key+": "+err.Error())
				continue
			}
			logger.LogInfoCtx(ctx, "[mail] parse/batch: ok key=%s subject=%q", key, parsed.Envelope.Subject)
			out.Results = append(out.Results, fromModelParsed(parsed))
		}
		logger.LogInfoCtx(ctx, "[mail] parse/batch: done ok=%d errors=%d", len(out.Results), len(out.Errors))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *Runtime) DecodeMessage(ctx context.Context, rawKey string) (*ParsedMessage, error) {
	var out ParsedMessage
	err := runIsolated(ctx, 0, func(ctx context.Context) error {
		logger.LogInfoCtx(ctx, "[mail] parse: start key=%s", rawKey)
		parsed, err := parser.ParseFromS3(ctx, rawKey)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] parse: failed key=%s err=%v", rawKey, err)
			return err
		}
		logger.LogInfoCtx(ctx, "[mail] parse: ok key=%s subject=%q from=%q attachments=%d",
			rawKey, parsed.Envelope.Subject, parsed.Envelope.From, len(parsed.Attachments))
		out = fromModelParsed(parsed)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *Runtime) ExtractStructured(ctx context.Context, s3ParsedKey, module string) (*StructuredExtractResult, error) {
	var out StructuredExtractResult
	err := runIsolated(ctx, 0, func(ctx context.Context) error {
		s3ParsedKey = strings.TrimSpace(s3ParsedKey)
		if s3ParsedKey == "" {
			return fmt.Errorf("s3_parsed_key is required")
		}
		raw, err := s3store.GetObjectBytes(ctx, s3ParsedKey)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] extract: read s3 key=%s err=%v", s3ParsedKey, err)
			return err
		}
		var parsed model.ParsedEmail
		if err := json.Unmarshal(raw, &parsed); err != nil {
			return fmt.Errorf("invalid parsed json: %w", err)
		}
		if err := extract.ValidateBody(parsed); err != nil {
			return err
		}
		intent, meta, confidence := extract.Run(module, parsed)
		out = StructuredExtractResult{
			Intent:            intent,
			ExtractedMetadata: meta,
			Confidence:        confidence,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}
