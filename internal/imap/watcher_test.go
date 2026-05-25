package imap

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/config"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/protocol"
)

func TestWatcherPollIssuesNoopBeforeFetch(t *testing.T) {
	addr, commands := startFakeIMAPServer(t, "IMAP4rev1 AUTH=PLAIN")
	cfg := &config.Config{
		PollIntervalMs:       1,
		ActivePollIntervalMs: 1,
		ActivePollDurationMs: 10,
	}
	acc := &config.AccountConfig{
		Name:       "no-idle",
		Host:       addr,
		Username:   "tit",
		Password:   "tit-pass",
		TLS:        "none",
		FolderRecv: "TunnelC2S",
	}
	w := NewWatcher(cfg, acc, func(protocol.Frame, string) {}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	sawNoop := false
	timeout := time.After(2 * time.Second)
	for {
		select {
		case cmd := <-commands:
			upper := strings.ToUpper(cmd)
			switch {
			case strings.Contains(upper, " NOOP"):
				sawNoop = true
			case strings.Contains(upper, " UID FETCH"):
				if !sawNoop {
					t.Fatalf("watcher issued UID FETCH before NOOP: %q", cmd)
				}
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("watcher did not stop after cancellation")
				}
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for watcher poll")
		}
	}
}

func TestWatcherDisableIdleForcesPollingWhenIdleAdvertised(t *testing.T) {
	addr, commands := startFakeIMAPServer(t, "IMAP4rev1 IDLE AUTH=PLAIN")
	cfg := &config.Config{
		PollIntervalMs:       1,
		ActivePollIntervalMs: 1,
		ActivePollDurationMs: 10,
		DisableIdle:          true,
	}
	acc := &config.AccountConfig{
		Name:       "disable-idle",
		Host:       addr,
		Username:   "tit",
		Password:   "tit-pass",
		TLS:        "none",
		FolderRecv: "TunnelC2S",
	}
	w := NewWatcher(cfg, acc, func(protocol.Frame, string) {}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	timeout := time.After(2 * time.Second)
	for {
		select {
		case cmd := <-commands:
			upper := strings.ToUpper(cmd)
			if strings.Contains(upper, " IDLE") {
				t.Fatalf("watcher used IDLE despite disable_idle: %q", cmd)
			}
			if strings.Contains(upper, " NOOP") {
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("watcher did not stop after cancellation")
				}
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for watcher NOOP")
		}
	}
}

func TestWatcherSubjectClientIDSkipsOtherClientBeforeBodyFetch(t *testing.T) {
	addr, commands := startSubjectFilterFakeIMAPServer(t)
	cfg := &config.Config{
		PollIntervalMs:       1,
		ActivePollIntervalMs: 1,
		ActivePollDurationMs: 10,
	}
	acc := &config.AccountConfig{
		Name:       "subject-filter",
		Host:       addr,
		Username:   "tit",
		Password:   "tit-pass",
		TLS:        "none",
		FolderRecv: "TunnelS2C",
	}
	w := NewWatcher(cfg, acc, func(protocol.Frame, string) {
		t.Fatal("handler should not receive another client's frame")
	}, nil)
	w.SubjectClientID = 7

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	timeout := time.After(2 * time.Second)
	for {
		select {
		case cmd := <-commands:
			upper := strings.ToUpper(cmd)
			if strings.Contains(upper, " UID FETCH") && strings.Contains(upper, "BODY") {
				t.Fatalf("watcher fetched body for another client's subject-tagged message: %q", cmd)
			}
			if strings.Contains(upper, " UID FETCH") && strings.Contains(upper, "ENVELOPE") {
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("watcher did not stop after cancellation")
				}
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for watcher subject ENVELOPE fetch")
		}
	}
}

func startFakeIMAPServer(t *testing.T, caps string) (string, <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake IMAP: %v", err)
	}
	commands := make(chan string, 32)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		_, _ = fmt.Fprintf(conn, "* OK no-idle test server\r\n")
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\r\n")
			select {
			case commands <- line:
			default:
			}
			tag, rest, _ := strings.Cut(line, " ")
			upper := strings.ToUpper(rest)
			switch {
			case strings.HasPrefix(upper, "CAPABILITY"):
				_, _ = fmt.Fprintf(conn, "* CAPABILITY %s\r\n%s OK CAPABILITY completed\r\n", caps, tag)
			case strings.HasPrefix(upper, "LOGIN"):
				_, _ = fmt.Fprintf(conn, "%s OK LOGIN completed\r\n", tag)
			case strings.HasPrefix(upper, "CREATE"):
				_, _ = fmt.Fprintf(conn, "%s NO [ALREADYEXISTS] mailbox exists\r\n", tag)
			case strings.HasPrefix(upper, "SELECT"):
				_, _ = fmt.Fprintf(conn, "* FLAGS (\\Seen \\Deleted)\r\n")
				_, _ = fmt.Fprintf(conn, "* 0 EXISTS\r\n")
				_, _ = fmt.Fprintf(conn, "* OK [UIDVALIDITY 1] UIDs valid\r\n")
				_, _ = fmt.Fprintf(conn, "* OK [UIDNEXT 1] next UID\r\n")
				_, _ = fmt.Fprintf(conn, "%s OK [READ-WRITE] SELECT completed\r\n", tag)
			case strings.HasPrefix(upper, "NOOP"):
				_, _ = fmt.Fprintf(conn, "%s OK NOOP completed\r\n", tag)
			case strings.HasPrefix(upper, "UID FETCH"):
				_, _ = fmt.Fprintf(conn, "%s OK FETCH completed\r\n", tag)
			case strings.HasPrefix(upper, "LOGOUT"):
				_, _ = fmt.Fprintf(conn, "* BYE logging out\r\n%s OK LOGOUT completed\r\n", tag)
				return
			default:
				_, _ = fmt.Fprintf(conn, "%s OK completed\r\n", tag)
			}
		}
	}()
	return ln.Addr().String(), commands
}

func startSubjectFilterFakeIMAPServer(t *testing.T) (string, <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake IMAP: %v", err)
	}
	commands := make(chan string, 64)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		_, _ = fmt.Fprintf(conn, "* OK subject-filter test server\r\n")
		sentEnvelope := false
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\r\n")
			select {
			case commands <- line:
			default:
			}
			tag, rest, _ := strings.Cut(line, " ")
			upper := strings.ToUpper(rest)
			switch {
			case strings.HasPrefix(upper, "CAPABILITY"):
				_, _ = fmt.Fprintf(conn, "* CAPABILITY IMAP4rev1 AUTH=PLAIN\r\n%s OK CAPABILITY completed\r\n", tag)
			case strings.HasPrefix(upper, "LOGIN"):
				_, _ = fmt.Fprintf(conn, "%s OK LOGIN completed\r\n", tag)
			case strings.HasPrefix(upper, "CREATE"):
				_, _ = fmt.Fprintf(conn, "%s NO [ALREADYEXISTS] mailbox exists\r\n", tag)
			case strings.HasPrefix(upper, "SELECT"):
				_, _ = fmt.Fprintf(conn, "* FLAGS (\\Seen \\Deleted)\r\n")
				_, _ = fmt.Fprintf(conn, "* 0 EXISTS\r\n")
				_, _ = fmt.Fprintf(conn, "* OK [UIDVALIDITY 1] UIDs valid\r\n")
				_, _ = fmt.Fprintf(conn, "* OK [UIDNEXT 1] next UID\r\n")
				_, _ = fmt.Fprintf(conn, "%s OK [READ-WRITE] SELECT completed\r\n", tag)
			case strings.HasPrefix(upper, "NOOP"):
				_, _ = fmt.Fprintf(conn, "%s OK NOOP completed\r\n", tag)
			case strings.HasPrefix(upper, "UID FETCH") && strings.Contains(upper, "ENVELOPE") && !sentEnvelope:
				sentEnvelope = true
				_, _ = fmt.Fprintf(conn, "* 1 FETCH (UID 1 ENVELOPE (NIL \"08 hello\" NIL NIL NIL NIL NIL NIL NIL NIL))\r\n")
				_, _ = fmt.Fprintf(conn, "%s OK FETCH completed\r\n", tag)
			case strings.HasPrefix(upper, "UID FETCH"):
				_, _ = fmt.Fprintf(conn, "%s OK FETCH completed\r\n", tag)
			case strings.HasPrefix(upper, "LOGOUT"):
				_, _ = fmt.Fprintf(conn, "* BYE logging out\r\n%s OK LOGOUT completed\r\n", tag)
				return
			default:
				_, _ = fmt.Fprintf(conn, "%s OK completed\r\n", tag)
			}
		}
	}()
	return ln.Addr().String(), commands
}
