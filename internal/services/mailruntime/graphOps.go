package mailruntime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/mailengine/inboxfilter"
	"CimplrCorpSaas/internal/mailengine/parser"
	"CimplrCorpSaas/internal/mailengine/pollcursor"
	"CimplrCorpSaas/internal/mailengine/s3store"
	"CimplrCorpSaas/internal/services/graphmail"
)

func (r *Runtime) VerifyGraph(ctx context.Context, conn GraphConnection) error {
	return runIsolated(ctx, 0, func(ctx context.Context) error {
		cfg, err := toGraphConfig(conn)
		if err != nil {
			return err
		}
		return graphmail.NewClientWithConfig(cfg).TestConnection(ctx)
	})
}

func (r *Runtime) PullGraphMessages(ctx context.Context, req GraphPullRequest) (*GraphPullResult, error) {
	var out GraphPullResult
	err := runIsolated(ctx, pullTimeout(), func(ctx context.Context) error {
		batch := req.PageSize
		if batch <= 0 {
			batch = 25
		}
		sinceStr := strings.TrimSpace(req.Since)
		if sinceStr == "" {
			now := pollcursor.FormatStored(time.Now().UTC())
			logger.LogInfoCtx(ctx, "[mail] graph/poll-page: init mailbox=%s sent=%v since=%s", req.Mailbox, req.SentFolder, now)
			out = GraphPullResult{Initialized: true, NewSince: now}
			return nil
		}
		since, err := pollcursor.ParseStored(sinceStr)
		if err != nil {
			return fmt.Errorf("invalid since timestamp")
		}

		cfg, err := toGraphConfig(req.Conn)
		if err != nil {
			return err
		}
		graphClient := graphmail.NewClientWithConfig(cfg)
		var listFn func(context.Context, string, time.Time, int) ([]graphmail.Message, error)
		if req.SentFolder {
			listFn = graphClient.ListSentMessagesSince
		} else {
			listFn = graphClient.ListInboxMessagesSince
		}
		graphMessages, err := listFn(ctx, req.Mailbox, since.UTC(), batch)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] graph/poll-page: list mailbox=%s err=%v", req.Mailbox, err)
			return err
		}

		skip := pollcursor.NewSkipSet(req.SkipMessageIDs)
		out = GraphPullResult{NewSince: sinceStr, Fetched: len(graphMessages)}
		maxCursor := since.UTC()
		skippedKnown, skippedFilter := 0, 0
		direction := inboxfilter.DirectionFromSentFolder(req.SentFolder)
		for _, gm := range graphMessages {
			ts := gm.CursorTime(req.SentFolder)
			if !ts.IsZero() && ts.After(maxCursor) {
				maxCursor = ts
			}
			if gm.ID == "" {
				continue
			}
			if skip.Has(gm.ID) {
				skippedKnown++
				continue
			}
			meta := inboxfilter.Input{
				From:                 gm.FromAddress(),
				To:                   gm.ToAddresses(),
				Subject:              gm.Subject,
				HasAttachments:       gm.HasAttachments,
				AttachmentNamesKnown: false,
			}
			if !inboxfilter.ShouldIngest(req.FiltersJSON, direction, meta) {
				skippedFilter++
				continue
			}
			raw, err := graphClient.GetMessageMIME(ctx, req.Mailbox, gm.ID)
			if err != nil {
				logger.LogErrorCtx(ctx, "[mail] graph/poll-page: fetch raw %s: %v", gm.ID, err)
				continue
			}
			rawKey := graphRawS3Key(req.Mailbox, req.SentFolder, gm.ID)
			if err := s3store.PutObject(ctx, rawKey, raw, "message/rfc822"); err != nil {
				logger.LogErrorCtx(ctx, "[mail] graph/poll-page: s3 upload %s: %v", rawKey, err)
				continue
			}
			parsed, err := parser.ParseFromS3(ctx, rawKey)
			if err != nil {
				logger.LogErrorCtx(ctx, "[mail] graph/poll-page: parse %s: %v", rawKey, err)
				continue
			}
			out.Messages = append(out.Messages, GraphPulledMessage{
				GraphMessageID: gm.ID,
				CursorTime:     pollcursor.FormatStored(ts.UTC()),
				Parsed:         fromModelParsed(parser.ForPollTransport(parsed)),
			})
		}
		out.NewSince = pollcursor.FormatStored(pollcursor.ResolveNewSince(since, maxCursor, len(graphMessages), skippedKnown))
		logger.LogInfoCtx(ctx, "[mail] graph/poll-page: mailbox=%s sent=%v fetched=%d messages=%d skipped_known=%d skipped_filter=%d since=%s new_since=%s",
			req.Mailbox, req.SentFolder, out.Fetched, len(out.Messages), skippedKnown, skippedFilter, sinceStr, out.NewSince)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}
