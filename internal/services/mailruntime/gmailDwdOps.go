package mailruntime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/mailengine/gmaildwd"
	"CimplrCorpSaas/internal/mailengine/inboxfilter"
	"CimplrCorpSaas/internal/mailengine/parser"
	"CimplrCorpSaas/internal/mailengine/pollcursor"
	"CimplrCorpSaas/internal/mailengine/s3store"
)

func toGmailDWDConfig(conn GmailDWDConnection) gmaildwd.ServiceAccountConfig {
	return gmaildwd.ServiceAccountConfig{
		ServiceAccountEmail: strings.TrimSpace(conn.ServiceAccountEmail),
		PrivateKey:          strings.TrimSpace(conn.PrivateKey),
		ClientID:            strings.TrimSpace(conn.ClientID),
	}
}

func (r *Runtime) VerifyGmailDWD(ctx context.Context, mailbox string, conn GmailDWDConnection) error {
	return runIsolated(ctx, 0, func(ctx context.Context) error {
		mailbox = strings.TrimSpace(strings.ToLower(mailbox))
		if mailbox == "" {
			return fmt.Errorf("mailbox_address is required")
		}
		cfg := toGmailDWDConfig(conn)
		token, err := gmaildwd.AccessToken(ctx, cfg, mailbox)
		if err != nil {
			return err
		}
		return gmaildwd.NewClient(mailbox, token).TestConnection(ctx)
	})
}

// PullGmailDWDMessages returns *GraphPullResult (not a Gmail-specific type) —
// same shape mailruntime has always used here for Gmail domain-wide-delegation
// pulls, so callers didn't need a new result type.
func (r *Runtime) PullGmailDWDMessages(ctx context.Context, req GmailDWDPullRequest) (*GraphPullResult, error) {
	var out GraphPullResult
	err := runIsolated(ctx, pullTimeout(), func(ctx context.Context) error {
		batch := req.PageSize
		if batch <= 0 {
			batch = 25
		}
		mailbox := strings.TrimSpace(strings.ToLower(req.Mailbox))
		sinceStr := strings.TrimSpace(req.Since)
		if sinceStr == "" {
			now := pollcursor.FormatStored(time.Now().UTC())
			logger.LogInfoCtx(ctx, "[mail] gmail-dwd/poll-page: init mailbox=%s sent=%v since=%s", mailbox, req.SentFolder, now)
			out = GraphPullResult{Initialized: true, NewSince: now}
			return nil
		}
		since, err := pollcursor.ParseStored(sinceStr)
		if err != nil {
			return fmt.Errorf("invalid since timestamp")
		}
		cfg := toGmailDWDConfig(req.Conn)
		token, err := gmaildwd.AccessToken(ctx, cfg, mailbox)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] gmail-dwd/poll-page: access token mailbox=%s err=%v", mailbox, err)
			return err
		}
		client := gmaildwd.NewClient(mailbox, token)
		ids, err := client.ListMessageIDsSince(ctx, req.SentFolder, since.UTC(), batch)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] gmail-dwd/poll-page: list mailbox=%s err=%v", mailbox, err)
			return err
		}
		skip := pollcursor.NewSkipSet(req.SkipMessageIDs)
		out = GraphPullResult{NewSince: sinceStr, Fetched: len(ids)}
		maxCursor := since.UTC()
		skippedKnown, skippedFilter := 0, 0
		direction := inboxfilter.DirectionFromSentFolder(req.SentFolder)
		for _, id := range ids {
			if skip.Has(id) {
				skippedKnown++
				continue
			}
			metaMsg, err := client.GetMessageMeta(ctx, id)
			if err != nil {
				logger.LogErrorCtx(ctx, "[mail] gmail-dwd/poll-page: meta %s: %v", id, err)
				continue
			}
			if !metaMsg.InternalDate.IsZero() && metaMsg.InternalDate.After(maxCursor) {
				maxCursor = metaMsg.InternalDate
			}
			meta := inboxfilter.Input{
				From:                 metaMsg.From,
				To:                   metaMsg.To,
				Subject:              metaMsg.Subject,
				HasAttachments:       metaMsg.HasAttachments,
				AttachmentNamesKnown: false,
			}
			if !inboxfilter.ShouldIngest(req.FiltersJSON, direction, meta) {
				skippedFilter++
				continue
			}
			rawMsg, err := client.GetRawMessage(ctx, id)
			if err != nil {
				logger.LogErrorCtx(ctx, "[mail] gmail-dwd/poll-page: fetch raw %s: %v", id, err)
				continue
			}
			if !rawMsg.InternalDate.IsZero() && rawMsg.InternalDate.After(maxCursor) {
				maxCursor = rawMsg.InternalDate
			}
			rawKey := gmailDWDRawS3Key(mailbox, req.SentFolder, id)
			if err := s3store.PutObject(ctx, rawKey, rawMsg.Raw, "message/rfc822"); err != nil {
				logger.LogErrorCtx(ctx, "[mail] gmail-dwd/poll-page: s3 upload %s: %v", rawKey, err)
				continue
			}
			parsed, err := parser.ParseFromS3(ctx, rawKey)
			if err != nil {
				logger.LogErrorCtx(ctx, "[mail] gmail-dwd/poll-page: parse %s: %v", rawKey, err)
				continue
			}
			cursor := rawMsg.InternalDate.UTC()
			if cursor.IsZero() {
				cursor = maxCursor
			}
			out.Messages = append(out.Messages, GraphPulledMessage{
				GraphMessageID: id,
				CursorTime:     pollcursor.FormatStored(cursor),
				Parsed:         fromModelParsed(parser.ForPollTransport(parsed)),
			})
		}
		out.NewSince = pollcursor.FormatStored(pollcursor.ResolveNewSince(since, maxCursor, len(ids), skippedKnown))
		logger.LogInfoCtx(ctx, "[mail] gmail-dwd/poll-page: mailbox=%s sent=%v fetched=%d messages=%d skipped_known=%d skipped_filter=%d new_since=%s",
			mailbox, req.SentFolder, out.Fetched, len(out.Messages), skippedKnown, skippedFilter, out.NewSince)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}
