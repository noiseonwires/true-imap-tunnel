package imap

import (
	"strings"
	"testing"
)

func TestXOAuth2InitialResponse(t *testing.T) {
	c := newXOAuth2Client("alice@example.com", "tok-abc")
	mech, ir, err := c.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if mech != "XOAUTH2" {
		t.Errorf("mech: got %q want XOAUTH2", mech)
	}
	want := "user=alice@example.com\x01auth=Bearer tok-abc\x01\x01"
	if string(ir) != want {
		t.Errorf("initial response:\n got %q\nwant %q", string(ir), want)
	}
}

func TestXOAuth2Rejects(t *testing.T) {
	if _, _, err := newXOAuth2Client("", "tok").Start(); err == nil {
		t.Error("expected error on empty username")
	}
	if _, _, err := newXOAuth2Client("u", "").Start(); err == nil {
		t.Error("expected error on empty token")
	}
}

func TestXOAuth2NextEmpty(t *testing.T) {
	c := newXOAuth2Client("u", "t")
	_, _, _ = c.Start()
	resp, err := c.Next([]byte(`{"status":"401"}`))
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(resp) != 0 {
		t.Errorf("Next: got %d bytes, want empty", len(resp))
	}
}

func TestFetchOAuthTokenStatic(t *testing.T) {
	tok, err := fetchOAuthToken(t.Context(), "  static-token\n", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if tok != "static-token" {
		t.Errorf("token: got %q want static-token", tok)
	}
}

func TestFetchOAuthTokenCommand(t *testing.T) {
	// Echo a token via the platform's default shell.
	var cmd string
	if isWindows() {
		cmd = "echo cmd-token"
	} else {
		cmd = "printf 'cmd-token'"
	}
	tok, err := fetchOAuthToken(t.Context(), "", cmd)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.Contains(tok, "cmd-token") {
		t.Errorf("token: got %q want substring 'cmd-token'", tok)
	}
}

func TestFetchOAuthTokenNeither(t *testing.T) {
	if _, err := fetchOAuthToken(t.Context(), "", ""); err == nil {
		t.Error("expected error when neither is set")
	}
}
