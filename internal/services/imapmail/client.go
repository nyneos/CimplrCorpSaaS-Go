package imapmail

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
)

// FetchedMessage is one raw RFC822 message from IMAP.
type FetchedMessage struct {
	UID       uint32
	Folder    string
	Raw       []byte
	Internal  time.Time
	MessageID string
}

// Client wraps go-imap for mailbox polling.
type Client struct{}

func NewClient() *Client { return &Client{} }

func (c *Client) dial(cfg Config) (*client.Client, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	var (
		imapClient *client.Client
		err        error
	)
	if cfg.UseTLS {
		imapClient, err = client.DialTLS(addr, &tls.Config{ServerName: cfg.Host})
	} else {
		imapClient, err = client.Dial(addr)
	}
	if err != nil {
		return nil, fmt.Errorf("imap dial %s: %w", addr, err)
	}
	if err := imapClient.Login(cfg.Username, cfg.Password); err != nil {
		_ = imapClient.Logout()
		return nil, fmt.Errorf("imap login: %w", err)
	}
	return imapClient, nil
}

// TestConnection verifies IMAP credentials and folder access.
func (c *Client) TestConnection(ctx context.Context, cfg Config, mailboxAddress string) error {
	if err := cfg.Resolve(mailboxAddress); err != nil {
		return err
	}
	imapClient, err := c.dial(cfg)
	if err != nil {
		return err
	}
	defer imapClient.Logout()

	done := make(chan error, 1)
	go func() {
		if _, err := imapClient.Select(cfg.InboxFolder, true); err != nil {
			done <- fmt.Errorf("select inbox folder %q: %w", cfg.InboxFolder, err)
			return
		}
		if cfg.SentFolder != "" {
			if _, err := imapClient.Select(cfg.SentFolder, true); err != nil {
				done <- fmt.Errorf("select sent folder %q: %w", cfg.SentFolder, err)
				return
			}
		}
		done <- nil
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// MaxUID returns the highest UID in folder (0 if empty).
func (c *Client) MaxUID(ctx context.Context, cfg Config, folder string) (uint32, error) {
	imapClient, err := c.dial(cfg)
	if err != nil {
		return 0, err
	}
	defer imapClient.Logout()

	type result struct {
		uid uint32
		err error
	}
	ch := make(chan result, 1)
	go func() {
		mbox, err := imapClient.Select(folder, true)
		if err != nil {
			ch <- result{err: err}
			return
		}
		if mbox.Messages == 0 {
			ch <- result{uid: 0}
			return
		}
		criteria := imap.NewSearchCriteria()
		uids, err := imapClient.UidSearch(criteria)
		if err != nil {
			ch <- result{err: err}
			return
		}
		var max uint32
		for _, u := range uids {
			if u > max {
				max = u
			}
		}
		ch <- result{uid: max}
	}()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case res := <-ch:
		return res.uid, res.err
	}
}

// FetchSinceUID fetches messages with UID > afterUID up to limit.
func (c *Client) FetchSinceUID(ctx context.Context, cfg Config, folder string, afterUID uint32, limit int) ([]FetchedMessage, error) {
	if limit <= 0 {
		limit = 25
	}
	imapClient, err := c.dial(cfg)
	if err != nil {
		return nil, err
	}
	defer imapClient.Logout()

	type result struct {
		msgs []FetchedMessage
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		if _, err := imapClient.Select(folder, false); err != nil {
			ch <- result{err: fmt.Errorf("select %q: %w", folder, err)}
			return
		}

		criteria := imap.NewSearchCriteria()
		if afterUID > 0 {
			criteria.Uid = new(imap.SeqSet)
			criteria.Uid.AddRange(afterUID+1, 0)
		}
		uids, err := imapClient.UidSearch(criteria)
		if err != nil {
			ch <- result{err: err}
			return
		}
		if len(uids) == 0 {
			ch <- result{msgs: nil}
			return
		}
		if len(uids) > limit {
			uids = uids[:limit]
		}

		seqset := new(imap.SeqSet)
		seqset.AddNum(uids...)

		section := &imap.BodySectionName{}
		items := []imap.FetchItem{section.FetchItem(), imap.FetchUid, imap.FetchInternalDate, imap.FetchEnvelope}
		messages := make(chan *imap.Message, len(uids))
		done := make(chan error, 1)
		go func() { done <- imapClient.UidFetch(seqset, items, messages) }()

		var out []FetchedMessage
		for msg := range messages {
			if msg == nil {
				continue
			}
			body := msg.GetBody(section)
			if body == nil {
				continue
			}
			raw, err := io.ReadAll(body)
			if err != nil {
				continue
			}
			fm := FetchedMessage{
				UID:      msg.Uid,
				Folder:   folder,
				Raw:      raw,
				Internal: msg.InternalDate,
			}
			if msg.Envelope != nil && msg.Envelope.MessageId != "" {
				fm.MessageID = strings.Trim(msg.Envelope.MessageId, "<>")
			}
			out = append(out, fm)
		}
		if err := <-done; err != nil {
			ch <- result{err: err}
			return
		}
		ch <- result{msgs: out}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		return res.msgs, res.err
	}
}
