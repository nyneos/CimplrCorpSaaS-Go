package mailruntime

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/mailengine/inboxfilter"
	"CimplrCorpSaas/internal/mailengine/parser"
	"CimplrCorpSaas/internal/mailengine/pollcursor"
	"CimplrCorpSaas/internal/mailengine/s3store"
	"CimplrCorpSaas/internal/services/imapmail"
)

func (r *Runtime) VerifyIMAP(ctx context.Context, mailbox string, conn IMAPConnection) error {
	return runIsolated(ctx, 0, func(ctx context.Context) error {
		cfg, err := toIMAPConfig(conn, mailbox)
		if err != nil {
			return err
		}
		return imapmail.NewClient().TestConnection(ctx, cfg, mailbox)
	})
}

func (r *Runtime) PullIMAPMessages(ctx context.Context, req IMAPPullRequest) (*IMAPPullResult, error) {
	var out IMAPPullResult
	err := runIsolated(ctx, pullTimeout(), func(ctx context.Context) error {
		batch := req.PageSize
		if batch <= 0 {
			batch = 25
		}
		cfg, err := toIMAPConfig(req.Conn, req.Mailbox)
		if err != nil {
			return err
		}
		folder := strings.TrimSpace(req.Folder)
		if folder == "" {
			folder = cfg.InboxFolder
		}
		client := imapmail.NewClient()
		lastUID := req.LastUID

		if lastUID == 0 {
			maxUID, err := client.MaxUID(ctx, cfg, folder)
			if err != nil {
				logger.LogErrorCtx(ctx, "[mail] imap/poll-folder: max-uid mailbox=%s folder=%s err=%v", req.Mailbox, folder, err)
				return err
			}
			initUID := maxUID
			if maxUID > 0 {
				// Leave cursor one behind max so the next poll ingests the newest
				// message instead of skipping it when init races with a just-sent mail.
				initUID = maxUID - 1
			}
			logger.LogInfoCtx(ctx, "[mail] imap/poll-folder: init mailbox=%s folder=%s uid=%d (max=%d)", req.Mailbox, folder, initUID, maxUID)
			out = IMAPPullResult{Initialized: true, NewLastUID: initUID}
			return nil
		}

		messages, err := client.FetchSinceUID(ctx, cfg, folder, lastUID, batch)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] imap/poll-folder: fetch mailbox=%s folder=%s err=%v", req.Mailbox, folder, err)
			return err
		}

		out = IMAPPullResult{NewLastUID: lastUID}
		skip := pollcursor.NewSkipSet(req.SkipIMAPMessageKeys)
		skippedKnown, skippedFilter := 0, 0
		direction := strings.TrimSpace(req.Direction)
		if direction == "" {
			direction = inboxfilter.DirectionReceived
		}
		for _, im := range messages {
			if im.UID > out.NewLastUID {
				out.NewLastUID = im.UID
			}
			imapKey := fmt.Sprintf("%s:%s:%d", req.InboxID, folder, im.UID)
			if skip.Has(imapKey) {
				skippedKnown++
				continue
			}
			meta, err := parser.MetaFromRaw(im.Raw)
			if err != nil {
				logger.LogErrorCtx(ctx, "[mail] imap/poll-folder: meta uid=%d: %v", im.UID, err)
				continue
			}
			if !inboxfilter.ShouldIngest(req.FiltersJSON, direction, meta) {
				skippedFilter++
				continue
			}
			rawKey := imapRawS3Key(req.Mailbox, req.Direction, imapKey)
			if err := s3store.PutObject(ctx, rawKey, im.Raw, "message/rfc822"); err != nil {
				logger.LogErrorCtx(ctx, "[mail] imap/poll-folder: s3 upload %s: %v", rawKey, err)
				continue
			}
			parsed, err := parser.ParseFromS3(ctx, rawKey)
			if err != nil {
				logger.LogErrorCtx(ctx, "[mail] imap/poll-folder: parse %s: %v", rawKey, err)
				continue
			}
			out.Messages = append(out.Messages, IMAPPulledMessage{
				UID:            im.UID,
				IMAPMessageKey: imapKey,
				Parsed:         fromModelParsed(parser.ForPollTransport(parsed)),
			})
		}
		logger.LogInfoCtx(ctx, "[mail] imap/poll-folder: mailbox=%s folder=%s messages=%d skipped_known=%d skipped_filter=%d new_uid=%d",
			req.Mailbox, folder, len(out.Messages), skippedKnown, skippedFilter, out.NewLastUID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}
