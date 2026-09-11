package imap

import (
	"bufio"
	"context"
	"fmt"
	imap "github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/noiseonwires/true-imap-tunnel/internal/config"
	"github.com/noiseonwires/true-imap-tunnel/internal/protocol"
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

func TestLazyExpungeUsesOldestPendingAge(t *testing.T) {
	watcher := NewWatcher(&config.Config{LazyExpungeMaxAgeMs: 1000}, &config.AccountConfig{}, nil, nil)
	now := time.Unix(100, 0)
	watcher.clock = func() time.Time { return now }
	watcher.addPendingExpunge(imap.UIDSetNum(4), 1)
	now = now.Add(900 * time.Millisecond)
	watcher.addPendingExpunge(imap.UIDSetNum(5), 1)
	if watcher.shouldFlushPendingExpunge() {
		t.Fatal("flushed before oldest deadline")
	}
	if wait := watcher.waitInterval(); wait != 100*time.Millisecond {
		t.Fatalf("wait=%v", wait)
	}
	now = now.Add(100 * time.Millisecond)
	if !watcher.shouldFlushPendingExpunge() {
		t.Fatal("new deletion postponed oldest deadline")
	}
	watcher.resetPendingGeneration(1)
	watcher.addPendingExpunge(imap.UIDSetNum(6), 1)
	watcher.resetPendingGeneration(1)
	if watcher.pendingCount != 1 {
		t.Fatal("same generation lost pending cleanup")
	}
	watcher.resetPendingGeneration(2)
	if watcher.pendingCount != 0 {
		t.Fatal("changed generation retained stale UIDs")
	}
}

func TestLazyExpungeDeadlineWithoutNewMail(t *testing.T) {
	for _, idle := range []bool{false, true} {
		for _, uidplus := range []bool{false, true} {
			t.Run(fmt.Sprintf("idle=%v/uidplus=%v", idle, uidplus), func(t *testing.T) {
				caps := "IMAP4rev1"
				if idle {
					caps += " IDLE"
				}
				if uidplus {
					caps += " UIDPLUS"
				}
				addr, commands := startFakeIMAPServer(t, caps)
				client, err := imapclient.DialInsecure(addr, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				if err := client.Login("test", "test").Wait(); err != nil {
					t.Fatal(err)
				}
				if _, err := client.Select("in", nil).Wait(); err != nil {
					t.Fatal(err)
				}
				watcher := NewWatcher(&config.Config{LazyExpungeMaxAgeMs: 20, PollIntervalMs: 5000}, &config.AccountConfig{}, nil, nil)
				watcher.client = client
				_, err = watcher.finishProcessRange(client, processResult{owned: imap.UIDSetNum(4, 8), ownedCount: 2}, true)
				if err != nil {
					t.Fatal(err)
				}
				if watcher.pendingCount != 2 {
					t.Fatal("batch flushed early")
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if idle {
					err = watcher.waitIdle(ctx, client)
				} else {
					err = watcher.waitPoll(ctx, client)
				}
				if err != nil {
					t.Fatal(err)
				}
				if !watcher.housekeeping || !watcher.shouldFlushPendingExpunge() {
					t.Fatal("quiet deletion deadline did not wake loop")
				}
				if err := watcher.flushPendingExpunge(); err != nil {
					t.Fatal(err)
				}
				if watcher.pendingCount != 0 || watcher.shouldFlushPendingExpunge() {
					t.Fatal("successful flush left deadline")
				}
				for {
					select {
					case command := <-commands:
						if strings.Contains(command, "FETCH") || strings.Contains(command, "NOOP") {
							t.Fatalf("housekeeping fetched/polled: %s", command)
						}
						if strings.Contains(command, "EXPUNGE") {
							if uidplus && !strings.Contains(command, "UID EXPUNGE 4,8") {
								t.Fatalf("UID ownership: %s", command)
							}
							return
						}
					case <-ctx.Done():
						t.Fatal("expunge command missing")
					}
				}
			})
		}
	}
}

func TestLazyExpungeFailureRetainsPending(t *testing.T) {
	addr, _ := startFakeIMAPServer(t, "IMAP4rev1 UIDPLUS")
	client, err := imapclient.DialInsecure(addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Login("test", "test").Wait(); err != nil {
		t.Fatal(err)
	}
	watcher := NewWatcher(&config.Config{}, &config.AccountConfig{}, nil, nil)
	watcher.client = client
	watcher.addPendingExpunge(imap.UIDSetNum(9), 1)
	_ = client.Close()
	if err := watcher.flushPendingExpunge(); err == nil || watcher.pendingCount != 1 {
		t.Fatalf("failed expunge lost pending: %v", err)
	}
	addr, _ = startFakeIMAPServer(t, "IMAP4rev1 UIDPLUS")
	client, err = imapclient.DialInsecure(addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Login("test", "test").Wait(); err != nil {
		t.Fatal(err)
	}
	watcher.client = client
	watcher.pendingStore = true
	if err := watcher.flushPendingExpunge(); err != nil || watcher.pendingCount != 0 {
		t.Fatalf("recovery failed: %v", err)
	}
}

func TestLazyExpungeStoreAndExpungeFailuresDoNotReplay(t *testing.T) {
	for _, failCommand := range []string{"UID STORE", "UID EXPUNGE"} {
		t.Run(failCommand, func(t *testing.T) {
			body := mustBuildTestMessage(t, protocol.Frame{Type: protocol.MsgData, StreamID: 1, SeqID: 1, Payload: []byte("once")})
			failed := false
			addr, _ := startFakeIMAPWithHook(t, "IMAP4rev1 UIDPLUS", func(tag, command string) (string, bool) {
				if strings.HasPrefix(command, failCommand) && !failed {
					failed = true
					return tag + " NO injected failure\r\n", true
				}
				if strings.HasPrefix(command, "UID FETCH 1:") {
					return fmt.Sprintf("* 1 FETCH (UID 42 BODY[] {%d}\r\n%s)\r\n%s OK fetched\r\n", len(body), body, tag), true
				}
				return "", false
			})
			client, err := imapclient.DialInsecure(addr, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if err := client.Login("test", "test").Wait(); err != nil {
				t.Fatal(err)
			}
			if _, err := client.Select("in", nil).Wait(); err != nil {
				t.Fatal(err)
			}
			delivered := 0
			watcher := NewWatcher(&config.Config{LazyExpungeThreshold_: 1}, &config.AccountConfig{}, func(frame protocol.Frame, _ string) {
				delivered++
				if string(frame.Payload) != "once" {
					t.Error("bad payload")
				}
			}, nil)
			watcher.client = client
			if err := watcher.fetchAndProcess(context.Background()); err == nil {
				t.Fatal("injected failure not observed")
			}
			if delivered != 1 || watcher.lastUID != 42 || watcher.pendingCount != 1 {
				t.Fatalf("delivery=%d cursor=%d pending=%d", delivered, watcher.lastUID, watcher.pendingCount)
			}
			if err := watcher.flushPendingExpunge(); err != nil {
				t.Fatal(err)
			}
			if err := watcher.fetchAndProcess(context.Background()); err != nil {
				t.Fatal(err)
			}
			if delivered != 1 || watcher.pendingCount != 0 || watcher.shouldFlushPendingExpunge() {
				t.Fatal("cleanup retry replayed payload or left expired work")
			}
		})
	}
}

func TestSafetyPollingBacksOffAndKicksCoalesce(t *testing.T) {
	watcher := NewWatcher(&config.Config{PollIntervalMs: 800, ActivePollIntervalMs: 100}, &config.AccountConfig{}, nil, nil)
	for count := 0; count < 100; count++ {
		watcher.Kick()
	}
	if len(watcher.kick) != 1 || watcher.Metrics().KickWakeups != 1 {
		t.Fatal("burst created multiple pending wakeups")
	}
	for empty, want := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 800 * time.Millisecond} {
		watcher.emptyPolls = empty
		if got := watcher.waitInterval(); got != want {
			t.Fatalf("empty=%d wait=%v want=%v", empty, got, want)
		}
	}
	watcher.emptyPolls = 0
	if watcher.waitInterval() != 100*time.Millisecond {
		t.Fatal("activity did not restore fast interval")
	}
}

func TestIdleNotificationAndMissingExistsReceiveProgress(t *testing.T) {
	for _, notify := range []bool{false, true} {
		t.Run(fmt.Sprintf("notify=%v", notify), func(t *testing.T) {
			body := mustBuildTestMessage(t, protocol.Frame{Type: protocol.MsgData, StreamID: 1, SeqID: 1, Payload: []byte("idle")})
			addr, _ := startFakeIMAPWithHook(t, "IMAP4rev1 IDLE", func(tag, command string) (string, bool) {
				if strings.HasPrefix(command, "UID FETCH 1:") {
					return fmt.Sprintf("* 1 FETCH (UID 1 BODY[] {%d}\r\n%s)\r\n%s OK fetched\r\n", len(body), body, tag), true
				}
				return "", false
			})
			client, err := imapclient.DialInsecure(addr, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if err := client.Login("test", "test").Wait(); err != nil {
				t.Fatal(err)
			}
			if _, err := client.Select("in", nil).Wait(); err != nil {
				t.Fatal(err)
			}
			interval := 20
			if notify {
				interval = 60000
			}
			delivered := 0
			watcher := NewWatcher(&config.Config{PollIntervalMs: interval}, &config.AccountConfig{}, func(protocol.Frame, string) { delivered++ }, nil)
			watcher.client = client
			if notify {
				watcher.notify <- struct{}{}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := watcher.waitIdle(ctx, client); err != nil {
				t.Fatal(err)
			}
			if err := watcher.fetchAndProcess(ctx); err != nil {
				t.Fatal(err)
			}
			if delivered != 1 {
				t.Fatalf("delivered=%d", delivered)
			}
		})
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

func TestWatcherIdleUsesBoundedSafetyFetch(t *testing.T) {
	addr, commands := startIdleFakeIMAPServer(t)
	cfg := &config.Config{
		PollIntervalMs:       1,
		ActivePollIntervalMs: 1,
		ActivePollDurationMs: 10,
	}
	acc := &config.AccountConfig{
		Name:       "idle-timeout",
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

	sawIdle := false
	sawDone := false
	timeout := time.After(2 * time.Second)
	for {
		select {
		case cmd := <-commands:
			upper := strings.ToUpper(cmd)
			if strings.Contains(upper, " IDLE") {
				sawIdle = true
			}
			if upper == "DONE" {
				sawDone = true
			}
			if strings.Contains(upper, " UID FETCH") {
				if !sawIdle || !sawDone {
					t.Fatalf("watcher fetched before bounded IDLE close: idle=%v done=%v cmd=%q",
						sawIdle, sawDone, cmd)
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
			t.Fatal("timed out waiting for bounded IDLE safety fetch")
		}
	}
}

func TestWatcherConsistencyOverlapCatchesDelayedLowerUID(t *testing.T) {
	frame1 := protocol.Frame{Type: protocol.MsgData, StreamID: 1, SeqID: 1, Payload: []byte("first")}
	frame2 := protocol.Frame{Type: protocol.MsgData, StreamID: 1, SeqID: 2, Payload: []byte("second")}
	body1 := mustBuildTestMessage(t, frame1)
	body2 := mustBuildTestMessage(t, frame2)
	addr := startDelayedVisibilityFakeIMAPServer(t, body1, body2)
	overlap := 8
	cfg := &config.Config{
		PollIntervalMs:       1,
		ActivePollIntervalMs: 1,
		ActivePollDurationMs: 10,
		FetchUIDOverlap_:     &overlap,
	}
	acc := &config.AccountConfig{
		Name:       "delayed-visibility",
		Host:       addr,
		Username:   "tit",
		Password:   "tit-pass",
		TLS:        "none",
		FolderRecv: "TunnelC2S",
	}

	received := make(chan protocol.Frame, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := NewWatcher(cfg, acc, func(f protocol.Frame, _ string) {
		received <- f
	}, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	seen := map[uint32]bool{}
	timeout := time.After(2 * time.Second)
	for len(seen) < 2 {
		select {
		case f := <-received:
			seen[f.SeqID] = true
		case <-timeout:
			t.Fatalf("timed out waiting for delayed UID repair; seen seqs=%v", seen)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watcher did not stop after cancellation")
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("seen seqs=%v, want seq 1 and 2", seen)
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

func mustBuildTestMessage(t *testing.T, f protocol.Frame) []byte {
	t.Helper()
	body, err := buildMessage(protocol.Encode(f), time.Now(), MessageOptions{
		Format:          "text",
		Subject:         config.DefaultMessageSubject,
		SubjectMode:     config.MessageSubjectModeFixed,
		From:            config.DefaultMessageFrom,
		To:              "receiver@example.com",
		SubjectClientID: true,
	})
	if err != nil {
		t.Fatalf("build test message: %v", err)
	}
	return body
}

func startIdleFakeIMAPServer(t *testing.T) (string, <-chan string) {
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
		_, _ = fmt.Fprintf(conn, "* OK idle test server\r\n")
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
				_, _ = fmt.Fprintf(conn, "* CAPABILITY IMAP4rev1 IDLE AUTH=PLAIN\r\n%s OK CAPABILITY completed\r\n", tag)
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
			case strings.HasPrefix(upper, "IDLE"):
				_, _ = fmt.Fprintf(conn, "+ idling\r\n")
				done, err := r.ReadString('\n')
				if err != nil {
					return
				}
				done = strings.TrimRight(done, "\r\n")
				select {
				case commands <- done:
				default:
				}
				_, _ = fmt.Fprintf(conn, "%s OK IDLE completed\r\n", tag)
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

func startDelayedVisibilityFakeIMAPServer(t *testing.T, uid1Body, uid2Body []byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake IMAP: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		_, _ = fmt.Fprintf(conn, "* OK delayed-visibility test server\r\n")
		deleted := map[uint32]bool{}
		bodyFetches := 0
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\r\n")
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
			case strings.HasPrefix(upper, "UID FETCH") && strings.Contains(upper, "FLAGS") && !strings.Contains(upper, "BODY"):
				writeFlagFetch(conn, 1, deleted[1])
				writeFlagFetch(conn, 2, deleted[2])
				_, _ = fmt.Fprintf(conn, "%s OK FETCH completed\r\n", tag)
			case strings.HasPrefix(upper, "UID FETCH") && strings.Contains(upper, "BODY"):
				bodyFetches++
				if bodyFetches == 1 {
					writeBodyFetch(conn, 2, uid2Body)
				} else {
					writeBodyFetch(conn, 1, uid1Body)
				}
				_, _ = fmt.Fprintf(conn, "%s OK FETCH completed\r\n", tag)
			case strings.HasPrefix(upper, "UID STORE"):
				if strings.Contains(upper, " 1 ") || strings.Contains(upper, " 1") {
					deleted[1] = true
				}
				if strings.Contains(upper, " 2 ") || strings.Contains(upper, " 2") {
					deleted[2] = true
				}
				_, _ = fmt.Fprintf(conn, "%s OK STORE completed\r\n", tag)
			case strings.HasPrefix(upper, "LOGOUT"):
				_, _ = fmt.Fprintf(conn, "* BYE logging out\r\n%s OK LOGOUT completed\r\n", tag)
				return
			default:
				_, _ = fmt.Fprintf(conn, "%s OK completed\r\n", tag)
			}
		}
	}()
	return ln.Addr().String()
}

func writeFlagFetch(conn net.Conn, uid uint32, deleted bool) {
	flags := "()"
	if deleted {
		flags = "(\\Deleted)"
	}
	_, _ = fmt.Fprintf(conn, "* %d FETCH (UID %d FLAGS %s)\r\n", uid, uid, flags)
}

func writeBodyFetch(conn net.Conn, uid uint32, body []byte) {
	_, _ = fmt.Fprintf(conn, "* %d FETCH (UID %d BODY[] {%d}\r\n", uid, uid, len(body))
	_, _ = conn.Write(body)
	_, _ = fmt.Fprintf(conn, ")\r\n")
}

func startFakeIMAPServer(t *testing.T, caps string) (string, <-chan string) {
	return startFakeIMAPWithHook(t, caps, nil)
}

func startFakeIMAPWithHook(t *testing.T, caps string, hook func(string, string) (string, bool)) (string, <-chan string) {
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
			if hook != nil {
				if response, handled := hook(tag, upper); handled {
					_, _ = io.WriteString(conn, response)
					continue
				}
			}
			switch {
			case strings.HasPrefix(upper, "IDLE"):
				_, _ = fmt.Fprint(conn, "+ idling\r\n")
				if _, err := r.ReadString('\n'); err != nil {
					return
				}
				_, _ = fmt.Fprintf(conn, "%s OK IDLE completed\r\n", tag)
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
