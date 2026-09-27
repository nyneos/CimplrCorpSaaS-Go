package mailruntime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/mailengine/gmailmail"
	"CimplrCorpSaas/internal/mailengine/inboxfilter"
	"CimplrCorpSaas/internal/mailengine/oauthmail"
	"CimplrCorpSaas/internal/mailengine/parser"
	"CimplrCorpSaas/internal/mailengine/pollcursor"
	"CimplrCorpSaas/internal/mailengine/s3store"
	"CimplrCorpSaas/internal/services/graphmail"
)

func (r *Runtime) OAuthAuthorizeURL(ctx context.Context, provider, transport, redirectURI, state string) (string, error) {
	var out string
	err := runIsolated(ctx, 0, func(ctx context.Context) error {
		u, err := oauthmail.AuthorizeURL(provider, transport, redirectURI, state)
		if err != nil {
			return err
		}
		out = u
		return nil
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func (r *Runtime) OAuthExchange(ctx context.Context, provider, transport, code, redirectURI string) (*OAuthExchangeResult, error) {
	var out OAuthExchangeResult
	err := runIsolated(ctx, 0, func(ctx context.Context) error {
		tokens, err := oauthmail.Exchange(ctx, provider, transport, code, redirectURI)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] oauth/exchange: provider=%s err=%v", provider, err)
			return err
		}
		email, _ := oauthmail.Identity(ctx, provider, tokens.AccessToken)
		out = OAuthExchangeResult{
			AccessToken:  tokens.AccessToken,
			RefreshToken: tokens.RefreshToken,
			ExpiresIn:    tokens.ExpiresIn,
			Scope:        tokens.Scope,
			Email:        email,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *Runtime) OAuthRefresh(ctx context.Context, provider, transport, refreshToken string) (*OAuthRefreshResult, error) {
	var out OAuthRefreshResult
	err := runIsolated(ctx, 0, func(ctx context.Context) error {
		tokens, err := oauthmail.Refresh(ctx, provider, transport, refreshToken)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] oauth/refresh: provider=%s err=%v", provider, err)
			return err
		}
		out = OAuthRefreshResult{
			AccessToken:  tokens.AccessToken,
			RefreshToken: tokens.RefreshToken,
			ExpiresIn:    tokens.ExpiresIn,
			Scope:        tokens.Scope,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *Runtime) VerifyOAuth(ctx context.Context, provider, accessToken string) error {
	return runIsolated(ctx, 0, func(ctx context.Context) error {
		switch strings.ToLower(strings.TrimSpace(provider)) {
		case oauthmail.ProviderMicrosoft:
			return graphmail.NewDelegatedClient(accessToken).TestConnection(ctx)
		case oauthmail.ProviderGoogle:
			return gmailmail.NewClient(accessToken).TestConnection(ctx)
		default:
			return fmt.Errorf("unsupported oauth provider %q", provider)
		}
	})
}

func (r *Runtime) PullOAuthMessages(ctx context.Context, req OAuthPullRequest) (*OAuthPullResult, error) {
	var out OAuthPullResult
	err := runIsolated(ctx, pullTimeout(), func(ctx context.Context) error {
		batch := req.PageSize
		if batch <= 0 {
			batch = 25
		}
		sinceStr := strings.TrimSpace(req.Since)
		if sinceStr == "" {
			now := pollcursor.FormatStored(time.Now().UTC())
			logger.LogInfoCtx(ctx, "[mail] oauth/poll-page: init mailbox=%s sent=%v since=%s", req.Mailbox, req.SentFolder, now)
			out = OAuthPullResult{Initialized: true, NewSince: now}
			return nil
		}
		since, err := pollcursor.ParseStored(sinceStr)
		if err != nil {
			return fmt.Errorf("invalid since timestamp")
		}

		var pullErr error
		switch strings.ToLower(strings.TrimSpace(req.Provider)) {
		case oauthmail.ProviderMicrosoft:
			out, pullErr = pullMicrosoftOAuthPage(ctx, req, since, batch)
		case oauthmail.ProviderGoogle:
			out, pullErr = pullGoogleOAuthPage(ctx, req, since, batch)
		default:
			pullErr = fmt.Errorf("unsupported oauth provider %q", req.Provider)
		}
		if pullErr != nil {
			return pullErr
		}
		logger.LogInfoCtx(ctx, "[mail] oauth/poll-page: mailbox=%s provider=%s sent=%v fetched=%d messages=%d since=%s",
			req.Mailbox, req.Provider, req.SentFolder, out.Fetched, len(out.Messages), out.NewSince)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func pullMicrosoftOAuthPage(ctx context.Context, req OAuthPullRequest, since time.Time, batch int) (OAuthPullResult, error) {
	client := graphmail.NewDelegatedClient(req.Conn.AccessToken)
	var listFn func(context.Context, time.Time, int) ([]graphmail.Message, error)
	if req.SentFolder {
		listFn = client.ListSentMessagesSince
	} else {
		listFn = client.ListInboxMessagesSince
	}
	msgs, err := listFn(ctx, since, batch)
	if err != nil {
		return OAuthPullResult{}, err
	}
	return ingestOAuthGraphMessages(ctx, req, since, msgs)
}

func ingestOAuthGraphMessages(ctx context.Context, req OAuthPullRequest, since time.Time, msgs []graphmail.Message) (OAuthPullResult, error) {
	skip := pollcursor.NewSkipSet(req.SkipMessageIDs)
	out := OAuthPullResult{NewSince: pollcursor.FormatStored(since), Fetched: len(msgs)}
	maxCursor := since.UTC()
	skippedKnown := 0
	direction := inboxfilter.DirectionFromSentFolder(req.SentFolder)
	client := graphmail.NewDelegatedClient(req.Conn.AccessToken)
	for _, gm := range msgs {
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
			continue
		}
		raw, err := client.GetMessageMIME(ctx, gm.ID)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] oauth/poll-page: fetch raw %s: %v", gm.ID, err)
			continue
		}
		rawKey := oauthRawS3Key(req.Mailbox, req.SentFolder, req.Provider, gm.ID)
		if err := s3store.PutObject(ctx, rawKey, raw, "message/rfc822"); err != nil {
			logger.LogErrorCtx(ctx, "[mail] oauth/poll-page: s3 upload %s: %v", rawKey, err)
			continue
		}
		parsed, err := parser.ParseFromS3(ctx, rawKey)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] oauth/poll-page: parse %s: %v", rawKey, err)
			continue
		}
		out.Messages = append(out.Messages, OAuthPulledMessage{
			ProviderMessageID: gm.ID,
			CursorTime:        pollcursor.FormatStored(ts.UTC()),
			Parsed:            fromModelParsed(parser.ForPollTransport(parsed)),
		})
	}
	out.NewSince = pollcursor.FormatStored(pollcursor.ResolveNewSince(since, maxCursor, len(msgs), skippedKnown))
	logger.LogInfoCtx(ctx, "[mail] oauth/poll-page: graph fetched=%d messages=%d skipped_known=%d", out.Fetched, len(out.Messages), skippedKnown)
	return out, nil
}

func pullGoogleOAuthPage(ctx context.Context, req OAuthPullRequest, since time.Time, batch int) (OAuthPullResult, error) {
	client := gmailmail.NewClient(req.Conn.AccessToken)
	ids, err := client.ListMessageIDsSince(ctx, req.SentFolder, since, batch)
	if err != nil {
		return OAuthPullResult{}, err
	}
	skip := pollcursor.NewSkipSet(req.SkipMessageIDs)
	out := OAuthPullResult{NewSince: pollcursor.FormatStored(since), Fetched: len(ids)}
	maxCursor := since.UTC()
	skippedKnown := 0
	direction := inboxfilter.DirectionFromSentFolder(req.SentFolder)
	for _, id := range ids {
		if skip.Has(id) {
			skippedKnown++
			continue
		}
		metaMsg, err := client.GetMessageMeta(ctx, id)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] oauth/poll-page: gmail meta %s: %v", id, err)
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
			continue
		}
		raw, err := client.GetRawMessage(ctx, id)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] oauth/poll-page: gmail fetch %s: %v", id, err)
			continue
		}
		if !raw.InternalDate.IsZero() && raw.InternalDate.After(maxCursor) {
			maxCursor = raw.InternalDate
		}
		rawKey := oauthRawS3Key(req.Mailbox, req.SentFolder, req.Provider, id)
		if err := s3store.PutObject(ctx, rawKey, raw.Raw, "message/rfc822"); err != nil {
			logger.LogErrorCtx(ctx, "[mail] oauth/poll-page: s3 upload %s: %v", rawKey, err)
			continue
		}
		parsed, err := parser.ParseFromS3(ctx, rawKey)
		if err != nil {
			logger.LogErrorCtx(ctx, "[mail] oauth/poll-page: parse %s: %v", rawKey, err)
			continue
		}
		cursor := maxCursor
		if !raw.InternalDate.IsZero() {
			cursor = raw.InternalDate
		}
		out.Messages = append(out.Messages, OAuthPulledMessage{
			ProviderMessageID: id,
			CursorTime:        pollcursor.FormatStored(cursor.UTC()),
			Parsed:            fromModelParsed(parser.ForPollTransport(parsed)),
		})
	}
	out.NewSince = pollcursor.FormatStored(pollcursor.ResolveNewSince(since, maxCursor, len(ids), skippedKnown))
	logger.LogInfoCtx(ctx, "[mail] oauth/poll-page: gmail fetched=%d messages=%d skipped_known=%d", out.Fetched, len(out.Messages), skippedKnown)
	return out, nil
}
