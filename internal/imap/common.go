// Package imap contains the IMAP transport: a sender that APPENDs frames
// and a watcher that uses IDLE + FETCH + EXPUNGE to receive them.
//
// One frame is carried per IMAP message. The frame bytes are base64-encoded
// and embedded in a minimal RFC 5322 message; the recipient parses the
// message, base64-decodes the body, and feeds the bytes into protocol.Decode.
package imap

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/emersion/go-imap/v2/imapclient"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/config"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/netprotect"
)

// messageMarker is the Subject line of every tunnel message. The value is
// deliberately innocuous and constant so IMAP servers don't apply any
// filtering or special handling.
const messageMarker = "TIT"

// base64LineLen is the wrap length used when base64-encoding frame bodies.
// 76 is the MIME standard.
const base64LineLen = 76

// MessageOptions controls the wire format of the IMAP message body
// carrying a frame.
type MessageOptions struct {
	// Format is "attachment" (Content-Type: application/octet-stream,
	// CTE base64, Content-Disposition: attachment) or "text"
	// (Content-Type: text/plain, CTE 7bit, body is literal base64).
	Format string

	// AttachmentFilename is the filename hint embedded in the
	// Content-Type / Content-Disposition headers when Format is
	// "attachment". Ignored for "text".
	AttachmentFilename string
}

// defaultMessageOptions returns the historical attachment-with-"tunnel.bin"
// behaviour. Used by tests that don't care which format is in use.
func defaultMessageOptions() MessageOptions {
	return MessageOptions{Format: "attachment", AttachmentFilename: "tunnel.bin"}
}

// buildMessage wraps a binary frame in an RFC 5322 message body suitable
// for APPEND. The output is small and self-contained.
//
// In "attachment" mode the body is presented as a binary attachment with
// the configured filename. In "text" mode it's a plain-text message
// whose body is the literal base64 encoding of the frame — visible as
// "normal" mail to a human inspecting the folder. Both modes are
// decoded identically by extractFrame (it just locates the body and
// base64-decodes it after stripping whitespace).
func buildMessage(frame []byte, date time.Time, opts MessageOptions) []byte {
	// Wrapped base64 of the frame.
	enc := base64.StdEncoding.EncodeToString(frame)
	var bodyB bytes.Buffer
	for len(enc) > base64LineLen {
		bodyB.WriteString(enc[:base64LineLen])
		bodyB.WriteString("\r\n")
		enc = enc[base64LineLen:]
	}
	bodyB.WriteString(enc)
	bodyB.WriteString("\r\n")

	var b bytes.Buffer
	b.WriteString("From: tunnel@localhost\r\n")
	b.WriteString("To: tunnel@localhost\r\n")
	b.WriteString("Subject: ")
	b.WriteString(messageMarker)
	b.WriteString("\r\n")
	b.WriteString("Date: ")
	b.WriteString(date.UTC().Format(time.RFC1123Z))
	b.WriteString("\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")

	switch opts.Format {
	case "text":
		// Plain-text body — what's between the headers and the end of
		// the message is the base64 of the frame, treated as opaque
		// ASCII text. No Content-Transfer-Encoding (defaults to 7bit;
		// the base64 alphabet is 7-bit clean by construction).
		b.WriteString("Content-Type: text/plain; charset=us-ascii\r\n")
	default:
		// Default: present as a binary attachment with the configured
		// filename. CTE: base64 means the message body is interpreted
		// as base64 by RFC-2045-aware clients.
		fn := opts.AttachmentFilename
		if fn == "" {
			fn = "tunnel.bin"
		}
		// Use quoted MIME parameter syntax for safety.
		b.WriteString("Content-Type: application/octet-stream; name=\"")
		b.WriteString(quoteMIMEParam(fn))
		b.WriteString("\"\r\n")
		b.WriteString("Content-Transfer-Encoding: base64\r\n")
		b.WriteString("Content-Disposition: attachment; filename=\"")
		b.WriteString(quoteMIMEParam(fn))
		b.WriteString("\"\r\n")
	}
	b.WriteString("\r\n")
	b.Write(bodyB.Bytes())
	return b.Bytes()
}

// quoteMIMEParam escapes characters that would break a quoted-string
// MIME parameter value: backslash and double-quote. Per RFC 2045 §5.1,
// a quoted-string parameter may include arbitrary chars except CR/LF,
// with " and \ escaped by \. We also strip CR/LF entirely.
func quoteMIMEParam(s string) string {
	var b bytes.Buffer
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '\r', '\n':
			// Drop — never legal here.
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// extractFrame extracts the binary frame from a fetched RFC 5322 body.
// It locates the header/body separator and base64-decodes the body.
//
// The implementation is intentionally tolerant: it accepts CRLF or LF
// separators and strips whitespace from the body before decoding.
func extractFrame(rawMessage []byte) ([]byte, error) {
	// Find the first blank line. RFC 5322 says CRLF CRLF; tolerate LF LF
	// because some servers normalise line endings.
	sep := findHeaderBodySeparator(rawMessage)
	if sep < 0 {
		return nil, errors.New("malformed message: no header/body separator")
	}
	body := rawMessage[sep:]
	// Strip all whitespace from base64 body before decoding.
	body = stripASCIIWhitespace(body)
	if len(body) == 0 {
		return nil, errors.New("empty message body")
	}
	out, err := base64.StdEncoding.DecodeString(string(body))
	if err != nil {
		return nil, fmt.Errorf("base64 decode: %w", err)
	}
	return out, nil
}

func findHeaderBodySeparator(b []byte) int {
	if i := bytes.Index(b, []byte("\r\n\r\n")); i >= 0 {
		return i + 4
	}
	if i := bytes.Index(b, []byte("\n\n")); i >= 0 {
		return i + 2
	}
	return -1
}

func stripASCIIWhitespace(b []byte) []byte {
	out := make([]byte, 0, len(b))
	for _, c := range b {
		switch c {
		case ' ', '\t', '\r', '\n':
		default:
			out = append(out, c)
		}
	}
	return out
}

// dialClient establishes an authenticated IMAP connection using the
// account's configured TLS mode and credentials.
func dialClient(acc *config.AccountConfig, opts *imapclient.Options) (*imapclient.Client, error) {
	if opts == nil {
		opts = &imapclient.Options{}
	}
	opts.Dialer = netprotect.WrapDialer(opts.Dialer)

	dialHost := acc.Host

	if opts.TLSConfig == nil && acc.InsecureSkipVerify {
		opts.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	} else if acc.InsecureSkipVerify && opts.TLSConfig != nil {
		opts.TLSConfig.InsecureSkipVerify = true
	}

	var (
		c   *imapclient.Client
		err error
	)
	switch acc.TLS {
	case "implicit", "":
		c, err = imapclient.DialTLS(dialHost, opts)
	case "starttls":
		c, err = imapclient.DialStartTLS(dialHost, opts)
	case "none":
		c, err = imapclient.DialInsecure(dialHost, opts)
	default:
		return nil, fmt.Errorf("unknown tls mode %q", acc.TLS)
	}
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", acc.Host, err)
	}

	if acc.UseOAuth2() {
		ctx, cancel := context.WithTimeout(context.Background(), tokenCommandTimeout)
		defer cancel()
		token, err := fetchOAuthToken(ctx, acc.OAuth2Token, acc.OAuth2TokenCommand)
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("oauth2 token for %s: %w", acc.Username, err)
		}
		if err := c.Authenticate(newXOAuth2Client(acc.Username, token)); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("xoauth2 %s: %w", acc.Username, err)
		}
		return c, nil
	}

	if err := c.Login(acc.Username, acc.Password).Wait(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("login %s: %w", acc.Username, err)
	}
	return c, nil
}

// ensureMailbox creates the folder if it does not already exist.
// "ALREADYEXISTS" responses are silently swallowed.
func ensureMailbox(c *imapclient.Client, mailbox string) error {
	if err := c.Create(mailbox, nil).Wait(); err != nil {
		// The library does not expose response codes neatly; rely on the
		// error string. Most servers respond NO with a TRYCREATE/ALREADYEXISTS
		// code; we treat that as success.
		msg := err.Error()
		if containsAny(msg, "ALREADYEXISTS", "already exists", "Mailbox exists", "EXISTS") {
			return nil
		}
		return err
	}
	return nil
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if bytes.Contains([]byte(s), []byte(sub)) {
			return true
		}
	}
	return false
}
