package imapmail

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/emersion/go-sasl"
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

type xoauth2Client struct {
	username string
	token    string
}

func (a *xoauth2Client) Start() (string, []byte, error) {
	msg := "user=" + a.username + "\x01auth=Bearer " + a.token + "\x01\x01"
	return "XOAUTH2", []byte(msg), nil
}

func (a *xoauth2Client) Next(_ []byte) ([]byte, error) {
	return nil, nil
}

var _ sasl.Client = (*xoauth2Client)(nil)

func plainIMAPAllowed(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func (c *Client) dial(ctx context.Context, cfg Config) (*client.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	dialer := &net.Dialer{Timeout: 20 * time.Second}
	tlsConfig := &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12}
	var (
		imapClient *client.Client
		err        error
	)
	if cfg.UseTLS {
		imapClient, err = client.DialWithDialerTLS(dialer, addr, tlsConfig)
	} else {
		imapClient, err = client.DialWithDialer(dialer, addr)
	}
	if err != nil {
		return nil, fmt.Errorf("imap dial %s: %w", addr, err)
	}
	imapClient.Timeout = 30 * time.Second
	if !cfg.UseTLS {
		ok, supportErr := imapClient.SupportStartTLS()
		if supportErr != nil {
			_ = imapClient.Terminate()
			return nil, fmt.Errorf("imap starttls probe %s: %w", addr, supportErr)
		}
		if ok {
			if err := imapClient.StartTLS(tlsConfig); err != nil {
				_ = imapClient.Terminate()
				return nil, fmt.Errorf("imap starttls %s: %w", addr, err)
			}
		} else if !plainIMAPAllowed(cfg.Host) {
			_ = imapClient.Terminate()
			return nil, fmt.Errorf("imap server %s does not support TLS", addr)
		}
	}
	var authErr error
	if strings.EqualFold(strings.TrimSpace(cfg.AuthMode), "oauth") {
		authErr = imapClient.Authenticate(&xoauth2Client{
			username: strings.TrimSpace(cfg.Username),
			token:    strings.TrimSpace(cfg.AccessToken),
		})
	} else {
		authErr = imapClient.Login(cfg.Username, cfg.Password)
	}
	if authErr != nil {
		_ = imapClient.Terminate()
		return nil, fmt.Errorf("imap login: %w", authErr)
	}
	return imapClient, nil
}

func (c *Client) run(ctx context.Context, cfg Config, fn func(*client.Client) error) error {
	imapClient, err := c.dial(ctx, cfg)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				done <- fmt.Errorf("imap panic: %v", rec)
			}
		}()
		done <- fn(imapClient)
	}()
	select {
	case <-ctx.Done():
		_ = imapClient.Terminate()
		return ctx.Err()
	case err := <-done:
		_ = imapClient.Logout()
		return err
	}
}

func (c *Client) TestConnection(ctx context.Context, cfg Config, mailboxAddress string) error {
	if err := cfg.Resolve(mailboxAddress); err != nil {
		return err
	}
	return c.run(ctx, cfg, func(imapClient *client.Client) error {
		if _, err := imapClient.Select(cfg.InboxFolder, true); err != nil {
			return fmt.Errorf("select inbox folder %q: %w", cfg.InboxFolder, err)
		}
		if cfg.SentFolder != "" {
			if _, err := imapClient.Select(cfg.SentFolder, true); err != nil {
				return fmt.Errorf("select sent folder %q: %w", cfg.SentFolder, err)
			}
		}
		return nil
	})
}

func (c *Client) MaxUID(ctx context.Context, cfg Config, folder string) (uint32, error) {
	var max uint32
	err := c.run(ctx, cfg, func(imapClient *client.Client) error {
		mbox, err := imapClient.Select(folder, true)
		if err != nil {
			return err
		}
		if mbox.Messages == 0 {
			return nil
		}
		uids, err := imapClient.UidSearch(imap.NewSearchCriteria())
		if err != nil {
			return err
		}
		for _, u := range uids {
			if u > max {
				max = u
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return max, nil
}

// FetchSinceUID fetches messages with UID > afterUID up to limit.
func (c *Client) FetchSinceUID(ctx context.Context, cfg Config, folder string, afterUID uint32, limit int) ([]FetchedMessage, error) {
	if limit <= 0 {
		limit = 25
	}
	var out []FetchedMessage
	err := c.run(ctx, cfg, func(imapClient *client.Client) error {
		if _, err := imapClient.Select(folder, false); err != nil {
			return fmt.Errorf("select %q: %w", folder, err)
		}
		criteria := imap.NewSearchCriteria()
		if afterUID > 0 {
			criteria.Uid = new(imap.SeqSet)
			criteria.Uid.AddRange(afterUID+1, 0)
		}
		uids, err := imapClient.UidSearch(criteria)
		if err != nil {
			return err
		}
		if len(uids) == 0 {
			return nil
		}
		if len(uids) > limit {
			uids = uids[:limit]
		}
		seqset := new(imap.SeqSet)
		seqset.AddNum(uids...)
		section := &imap.BodySectionName{}
		items := []imap.FetchItem{section.FetchItem(), imap.FetchUid, imap.FetchInternalDate, imap.FetchEnvelope}
		messages := make(chan *imap.Message, len(uids))
		fetchDone := make(chan error, 1)
		go func() { fetchDone <- imapClient.UidFetch(seqset, items, messages) }()
		for msg := range messages {
			if msg == nil {
				continue
			}
			body := msg.GetBody(section)
			if body == nil {
				continue
			}
			raw, err := io.ReadAll(io.LimitReader(body, 25<<20))
			if err != nil {
				return fmt.Errorf("read imap message %d: %w", msg.Uid, err)
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
		return <-fetchDone
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
