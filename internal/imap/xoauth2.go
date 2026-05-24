package imap

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/emersion/go-sasl"
)

// xoauth2Client is a minimal SASL XOAUTH2 client. It is used by Microsoft
// (outlook.com, Office 365) and Google (Gmail) IMAP servers and is
// distinct from RFC 7628 OAUTHBEARER (which most Microsoft servers do
// NOT accept on IMAP).
//
// The mechanism is one-shot ("client-first, no challenge"): the initial
// response carries the entire authentication payload, and the server
// either accepts (tagged OK) or rejects with a continuation that wraps
// a JSON error blob. We forward an empty response to any continuation
// so the IMAP layer surfaces the tagged error rather than hanging.
//
// Wire format of the initial response (before SASL's base64):
//
//	"user=" <user> ^A "auth=Bearer " <token> ^A ^A
//
// where ^A is the ASCII 0x01 byte.
type xoauth2Client struct {
	username, token string
}

func newXOAuth2Client(username, token string) sasl.Client {
	return &xoauth2Client{username: username, token: token}
}

func (c *xoauth2Client) Start() (string, []byte, error) {
	if c.username == "" {
		return "", nil, errors.New("xoauth2: empty username")
	}
	if c.token == "" {
		return "", nil, errors.New("xoauth2: empty token")
	}
	const sep = "\x01"
	payload := "user=" + c.username + sep + "auth=Bearer " + c.token + sep + sep
	return "XOAUTH2", []byte(payload), nil
}

func (c *xoauth2Client) Next(challenge []byte) ([]byte, error) {
	// XOAUTH2 only produces a continuation on failure; the server's
	// continuation payload is base64-encoded JSON with the error
	// details. We have nothing useful to send in reply — an empty
	// client response terminates the exchange so the server emits the
	// tagged NO/BAD with its error code attached.
	return []byte{}, nil
}

// fetchOAuthToken returns the current OAuth2 access token for an
// account. If acc.OAuth2Token is set, that static value is returned.
// Otherwise, if acc.OAuth2TokenCommand is set, the configured command
// is executed and its trimmed stdout is returned. The command is
// re-run on every (re)connect so external refreshers can keep the
// token fresh without coordinating with this process.
func fetchOAuthToken(ctx context.Context, token, cmdline string) (string, error) {
	if token != "" {
		return strings.TrimSpace(token), nil
	}
	if cmdline == "" {
		return "", errors.New("no oauth2 token configured")
	}
	c := exec.CommandContext(ctx, "sh", "-c", cmdline)
	if isWindows() {
		c = exec.CommandContext(ctx, "cmd", "/C", cmdline)
	}
	out, err := c.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("oauth2_token_command failed: %v: %s",
				err, strings.TrimSpace(string(ee.Stderr)))
		}
		return "", fmt.Errorf("oauth2_token_command: %w", err)
	}
	tok := strings.TrimSpace(string(out))
	if tok == "" {
		return "", errors.New("oauth2_token_command produced empty output")
	}
	return tok, nil
}

// tokenCommandTimeout bounds how long we wait for an oauth2_token_command.
const tokenCommandTimeout = 30 * time.Second
