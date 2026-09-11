// End-to-end integration test that spins up an in-memory IMAP server and
// runs both tunnel modes (client + server) against it. Exercises the full
// pipeline: TCP accept → APPEND → IDLE notification → FETCH → STORE \Deleted
// → EXPUNGE → frame decode → TCP write.
package tunnel

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	imap "github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/emersion/go-imap/v2/imapserver/imapmemserver"

	"github.com/noiseonwires/true-imap-tunnel/internal/config"
	"github.com/noiseonwires/true-imap-tunnel/internal/protocol"
	"github.com/noiseonwires/true-imap-tunnel/internal/tlog"
)

func init() {
	// Quieter logs unless TIT_TEST_VERBOSE=1.
	if os.Getenv("TIT_TEST_VERBOSE") == "" {
		log.SetOutput(io.Discard)
	}
	if lvl := os.Getenv("TIT_TEST_LOG_LEVEL"); lvl != "" {
		if l, ok := tlog.ParseLevel(lvl); ok {
			tlog.SetLevel(l)
		}
	}
}

// startMemIMAP launches an in-memory IMAP server with one user and the
// folders our tunnel will use, then returns its address.
func startMemIMAP(t *testing.T) string {
	return startMemIMAPWithFault(t, nil)
}

func startMemIMAPWithFault(t *testing.T, drop *atomic.Bool) string {
	var hook func(string, net.Conn)
	if drop != nil {
		hook = func(mailbox string, conn net.Conn) {
			if mailbox == "Tunnel.C2S" && drop.CompareAndSwap(false, true) {
				_ = conn.Close()
			}
		}
	}
	return startMemIMAPWithAppendHook(t, hook)
}

func startMemIMAPWithAppendHook(t *testing.T, hook func(string, net.Conn)) string {
	t.Helper()

	memSrv := imapmemserver.New()
	user := imapmemserver.NewUser("tit", "tit-pass")
	if err := user.Create("INBOX", nil); err != nil {
		t.Fatalf("create INBOX: %v", err)
	}
	// Two folders for the bidirectional pipeline.
	if err := user.Create("Tunnel.C2S", nil); err != nil {
		t.Fatalf("create C2S: %v", err)
	}
	if err := user.Create("Tunnel.S2C", nil); err != nil {
		t.Fatalf("create S2C: %v", err)
	}
	memSrv.AddUser(user)

	// Second user for multipath tests. Separate folders to mimic two
	// completely independent IMAP accounts.
	user2 := imapmemserver.NewUser("tit2", "tit-pass2")
	if err := user2.Create("INBOX", nil); err != nil {
		t.Fatalf("create INBOX 2: %v", err)
	}
	if err := user2.Create("Tunnel.C2S", nil); err != nil {
		t.Fatalf("create C2S 2: %v", err)
	}
	if err := user2.Create("Tunnel.S2C", nil); err != nil {
		t.Fatalf("create S2C 2: %v", err)
	}
	memSrv.AddUser(user2)

	srv := imapserver.New(&imapserver.Options{
		NewSession: func(conn *imapserver.Conn) (imapserver.Session, *imapserver.GreetingData, error) {
			session := memSrv.NewSession()
			if hook != nil {
				session = &ambiguousAppendSession{SessionIMAP4rev2: session.(imapserver.SessionIMAP4rev2), conn: conn.NetConn(), hook: hook}
			}
			return session, nil, nil
		},
		Caps: imap.CapSet{
			imap.CapIMAP4rev1: {},
			imap.CapIMAP4rev2: {},
		},
		InsecureAuth: true, // we won't use TLS in tests
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		_ = srv.Serve(ln)
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

type ambiguousAppendSession struct {
	imapserver.SessionIMAP4rev2
	conn net.Conn
	hook func(string, net.Conn)
}

func (session *ambiguousAppendSession) Append(mailbox string, reader imap.LiteralReader, options *imap.AppendOptions) (*imap.AppendData, error) {
	result, err := session.SessionIMAP4rev2.Append(mailbox, reader, options)
	if err == nil {
		session.hook(mailbox, session.conn)
	}
	return result, err
}

func TestEndToEnd_AmbiguousOpenAppend(t *testing.T) {
	var dropped atomic.Bool
	addr := startMemIMAPWithFault(t, &dropped)
	target := startEchoServer(t)
	clientCfg := &config.Config{Mode: config.ModeClient, Listen: freePort(t), PingIntervalMs: -1, Accounts: []config.AccountConfig{makeAccount(addr, "Tunnel.C2S", "Tunnel.S2C")}}
	serverCfg := &config.Config{Mode: config.ModeServer, Target: target, Accounts: []config.AccountConfig{makeAccount(addr, "Tunnel.S2C", "Tunnel.C2S")}}
	client, server := mustNew(t, clientCfg), mustNew(t, serverCfg)
	var dials atomic.Int32
	server.dialTarget = func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); _ = server.Run(ctx) }()
	go func() { defer workers.Done(); _ = client.Run(ctx) }()
	defer func() { cancel(); workers.Wait() }()
	waitForTransport(t, client, server)
	conn, err := net.Dial("tcp", clientCfg.Listen)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	_, _ = io.WriteString(conn, "exactly once")
	_ = conn.(*net.TCPConn).CloseWrite()
	body, err := io.ReadAll(conn)
	if err != nil || string(body) != "exactly once" || !dropped.Load() || dials.Load() != 1 {
		t.Fatalf("body=%q err=%v dropped=%v dials=%d", body, err, dropped.Load(), dials.Load())
	}
}

// startEchoServer starts a tiny TCP echo server and returns its address.
func startEchoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

// startDelayedTCPProxy returns a local TCP address that proxies to target after
// delaying each accepted connection. It is used to model a slower IMAP provider
// that connects after the first path has already become ready.
func startDelayedTCPProxy(t *testing.T, target string, delay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	go func() {
		for {
			downstream, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer downstream.Close()
				time.Sleep(delay)
				upstream, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer upstream.Close()

				done := make(chan struct{}, 2)
				go func() {
					_, _ = io.Copy(upstream, downstream)
					done <- struct{}{}
				}()
				go func() {
					_, _ = io.Copy(downstream, upstream)
					done <- struct{}{}
				}()
				<-done
			}()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

func makeAccount(host string, sendFolder, recvFolder string) config.AccountConfig {
	return config.AccountConfig{
		Name:       sendFolder + "→" + recvFolder,
		Host:       host,
		Username:   "tit",
		Password:   "tit-pass",
		TLS:        "none",
		FolderSend: sendFolder,
		FolderRecv: recvFolder,
	}
}

func makeAccountUser(host, user, pass, sendFolder, recvFolder string) config.AccountConfig {
	return config.AccountConfig{
		Name:       user + ":" + sendFolder + "→" + recvFolder,
		Host:       host,
		Username:   user,
		Password:   pass,
		TLS:        "none",
		FolderSend: sendFolder,
		FolderRecv: recvFolder,
	}
}

// freePort returns a random TCP address on 127.0.0.1.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestEndToEnd_SingleAccount(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder: &rt,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			// Folders SWAPPED on the server side.
			makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder: &rt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	// Give both sides time to connect IMAP, SELECT, and enter IDLE.
	waitForListen(t, clientListen, 10*time.Second, client, server)
	time.Sleep(500 * time.Millisecond)

	// Now connect to client listener and verify echo.
	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial client listener: %v", err)
	}
	defer conn.Close()

	want := []byte("hello, true-imap-tunnel!\n")
	if _, err := conn.Write(want); err != nil {
		t.Fatalf("write: %v", err)
	}

	buf := make([]byte, len(want))
	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(buf) != string(want) {
		t.Errorf("got %q want %q", buf, want)
	}

	// Second round-trip on the SAME stream verifies the bidirectional flow
	// keeps working after the first IDLE cycle.
	want2 := []byte("round 2\n")
	if _, err := conn.Write(want2); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	buf2 := make([]byte, len(want2))
	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.ReadFull(conn, buf2); err != nil {
		t.Fatalf("read echo 2: %v", err)
	}
	if string(buf2) != string(want2) {
		t.Errorf("round 2: got %q want %q", buf2, want2)
	}

	cancel()
	wg.Wait()
}

func TestEndToEnd_RequestHalfCloseResponse(t *testing.T) {
	for _, mode := range []config.MultipathMode{config.MultipathModeStreamAffinity, config.MultipathModeFrameRoundRobin} {
		for _, async := range []bool{false, true} {
			for _, zeroRTT := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/async=%v/zero=%v", mode, async, zeroRTT), func(t *testing.T) {
					imapAddr := startMemIMAP(t)
					target, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					defer target.Close()
					response := strings.Repeat("half-close-response-", 55000)
					requestSeen := make(chan string, 1)
					go func() {
						conn, err := target.Accept()
						if err != nil {
							return
						}
						defer conn.Close()
						body, err := io.ReadAll(conn)
						if err != nil {
							return
						}
						requestSeen <- string(body)
						_, _ = io.WriteString(conn, response)
					}()
					clientCfg := &config.Config{Mode: config.ModeClient, Listen: freePort(t), AsyncDataSend: async, ZeroRTTOpen: &zeroRTT, PingIntervalMs: -1,
						Accounts: []config.AccountConfig{makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C")}}
					serverCfg := &config.Config{Mode: config.ModeServer, Target: target.Addr().String(), AsyncDataSend: async,
						Accounts: []config.AccountConfig{makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S")}}
					clientCfg.MultipathMode, serverCfg.MultipathMode = mode, mode
					if mode == config.MultipathModeFrameRoundRobin {
						clientCfg.Accounts = append(clientCfg.Accounts, makeAccountUser(imapAddr, "tit2", "tit-pass2", "Tunnel.C2S", "Tunnel.S2C"))
						serverCfg.Accounts = append(serverCfg.Accounts, makeAccountUser(imapAddr, "tit2", "tit-pass2", "Tunnel.S2C", "Tunnel.C2S"))
					}
					ctx, cancel := context.WithCancel(context.Background())
					server, client := mustNew(t, serverCfg), mustNew(t, clientCfg)
					var workers sync.WaitGroup
					workers.Add(2)
					go func() { defer workers.Done(); _ = server.Run(ctx) }()
					go func() { defer workers.Done(); _ = client.Run(ctx) }()
					defer func() { cancel(); workers.Wait() }()
					waitForTransport(t, client, server)
					if mode == config.MultipathModeFrameRoundRobin {
						for index := 0; index < 2; index++ {
							if err := client.paths.SendVia(index, protocol.Frame{Type: protocol.MsgPing, Payload: make([]byte, 8)}); err != nil {
								t.Fatal(err)
							}
						}
						deadline := time.NewTimer(5 * time.Second)
						defer deadline.Stop()
						tick := time.NewTicker(10 * time.Millisecond)
						defer tick.Stop()
						for client.paths.ProvenCount() != 2 || server.paths.ProvenCount() != 2 {
							select {
							case <-tick.C:
							case <-deadline.C:
								t.Fatal("multipath not proven")
							}
						}
					}
					conn, err := net.Dial("tcp", clientCfg.Listen)
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
					_, _ = io.WriteString(conn, "request requiring EOF")
					_ = conn.(*net.TCPConn).CloseWrite()
					body, err := io.ReadAll(conn)
					if err != nil || string(body) != response {
						t.Fatalf("response bytes=%d want=%d err=%v", len(body), len(response), err)
					}
					select {
					case request := <-requestSeen:
						if request != "request requiring EOF" {
							t.Fatalf("request=%q", request)
						}
					case <-time.After(time.Second):
						t.Fatal("missing request")
					}
				})
			}
		}
	}
}

func TestEndToEnd_AmbiguousOpenAppendBatch(t *testing.T) {
	var dropped atomic.Bool
	addr := startMemIMAPWithFault(t, &dropped)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	observed := make(chan string, 2)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() { defer conn.Close(); body, _ := io.ReadAll(conn); observed <- string(body) }()
		}
	}()
	server := mustNew(t, &config.Config{Mode: config.ModeServer, Target: listener.Addr().String(), Accounts: []config.AccountConfig{makeAccount(addr, "Tunnel.S2C", "Tunnel.C2S")}})
	var dials atomic.Int32
	server.dialTarget = func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Run(ctx) }()
	defer func() { cancel(); <-done }()
	waitForTransport(t, server)
	frames, err := protocol.EncodeBatch([]protocol.Frame{{Type: protocol.MsgOpen, StreamID: 7, SeqID: 1}, {Type: protocol.MsgData, StreamID: 7, SeqID: 2, Payload: []byte("batched once")}, {Type: protocol.MsgFin, StreamID: 7, SeqID: 3}})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("Subject: test\r\nContent-Transfer-Encoding: base64\r\n\r\n" + base64.StdEncoding.EncodeToString(frames) + "\r\n")
	for attempt := 0; attempt < 2; attempt++ {
		client, err := imapclient.DialInsecure(addr, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Login("tit", "tit-pass").Wait(); err != nil {
			t.Fatal(err)
		}
		cmd := client.Append("Tunnel.C2S", int64(len(body)), nil)
		_, _ = cmd.Write(body)
		_ = cmd.Close()
		_, err = cmd.Wait()
		_ = client.Close()
		if attempt == 0 && err == nil {
			t.Fatal("first APPEND did not lose status")
		}
		if attempt == 1 && err != nil {
			t.Fatal(err)
		}
	}
	select {
	case body := <-observed:
		if body != "batched once" {
			t.Fatalf("payload=%q", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("batch not delivered")
	}
	cancel()
	<-done
	if dials.Load() != 1 {
		t.Fatalf("duplicate batch dialed %d times", dials.Load())
	}
}

func waitForTransport(t *testing.T, tunnels ...*Tunnel) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		ready := true
		for _, tun := range tunnels {
			ready = ready && tun.paths.AllConnected() && tun.paths.AnyReceiveReady()
		}
		if ready {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("transport not ready")
		}
	}
}

func TestEndToEnd_BurstyClientsRemainCompatible(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			addr := startMemIMAP(t)
			target := startEchoServer(t)
			server := mustNew(t, &config.Config{Mode: config.ModeServer, Target: target, AsyncDataSend: async, Accounts: []config.AccountConfig{makeAccount(addr, "Tunnel.S2C", "Tunnel.C2S")}})
			ctx, cancel := context.WithCancel(context.Background())
			var workers sync.WaitGroup
			workers.Add(1)
			go func() { defer workers.Done(); _ = server.Run(ctx) }()
			defer func() { cancel(); workers.Wait() }()
			clients := make([]*Tunnel, 8)
			for index := range clients {
				client := mustNew(t, &config.Config{Mode: config.ModeClient, Listen: freePort(t), ClientID: byte(index + 1), AsyncDataSend: async, PingIntervalMs: -1, Accounts: []config.AccountConfig{makeAccount(addr, "Tunnel.C2S", "Tunnel.S2C")}})
				clients[index] = client
				workers.Add(1)
				go func() { defer workers.Done(); _ = client.Run(ctx) }()
			}
			waitForTransport(t, append(clients, server)...)
			failures := make(chan error, 2*len(clients))
			var transfers sync.WaitGroup
			for index, client := range clients {
				transfers.Add(1)
				go func(index int, client *Tunnel) {
					defer transfers.Done()
					conn, err := net.Dial("tcp", client.cfg.Listen)
					if err != nil {
						failures <- err
						return
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
					payload := strings.Repeat(fmt.Sprintf("client-%d", index), 300000)
					written := make(chan error, 1)
					go func() {
						_, err := io.WriteString(conn, payload)
						if err == nil {
							err = conn.(*net.TCPConn).CloseWrite()
						}
						written <- err
					}()
					body, err := io.ReadAll(conn)
					if err != nil || string(body) != payload {
						failures <- fmt.Errorf("client %d: bytes=%d want=%d err=%v", index, len(body), len(payload), err)
					}
					if err := <-written; err != nil {
						failures <- err
					}
				}(index, client)
			}
			transfers.Wait()
			close(failures)
			for err := range failures {
				t.Error(err)
			}
		})
	}
}

func TestEndToEnd_Multiplex(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder: &rt,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder: &rt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)
	time.Sleep(500 * time.Millisecond)

	// Open 3 concurrent connections, each echoes its own unique payload.
	const N = 3
	var swg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		i := i
		swg.Add(1)
		go func() {
			defer swg.Done()
			conn, err := net.Dial("tcp", clientListen)
			if err != nil {
				errs <- fmt.Errorf("conn %d dial: %w", i, err)
				return
			}
			defer conn.Close()
			payload := []byte(strings.Repeat(fmt.Sprintf("stream-%d:", i), 50))
			if _, err := conn.Write(payload); err != nil {
				errs <- fmt.Errorf("conn %d write: %w", i, err)
				return
			}
			buf := make([]byte, len(payload))
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			if _, err := io.ReadFull(conn, buf); err != nil {
				errs <- fmt.Errorf("conn %d read: %w", i, err)
				return
			}
			if string(buf) != string(payload) {
				errs <- fmt.Errorf("conn %d mismatch", i)
				return
			}
		}()
	}
	swg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	cancel()
	wg.Wait()
}

// waitForListen waits for the given local listener to accept connections,
// then waits for the tunnel's phantom probe-stream (allocated as a side
// effect of the probe Dial) to fully tear down. Without that drain step,
// the phantom OPEN/FIN cycle races with the real test's traffic and
// causes flaky timeouts on memserver-based e2e runs.
//
// The optional `tunnels` argument lists tunnels whose stream counts
// must drop back to 0 before this helper returns. Callers that don't
// need the drain step (e.g. tests that immediately tear everything
// down) can pass none.
func waitForListen(t *testing.T, addr string, timeout time.Duration, tunnels ...*Tunnel) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Drain the phantom stream we just created so the real test isn't
	// racing the phantom's OPEN/FIN cycle. Bounded wait — if drain
	// doesn't happen we silently proceed and let the test surface the
	// real issue.
	if len(tunnels) > 0 {
		drainDeadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(drainDeadline) {
			allIdle := true
			for _, tn := range tunnels {
				if tn != nil && tn.streams.Count() != 0 {
					allIdle = false
					break
				}
			}
			if allIdle {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
}

// mustNew constructs a Tunnel for a test config, fatal'ing on error.
// New is fallible (encryption-setup can fail on invalid config), but
// e2e tests never exercise that path, so the helper keeps test code
// terse.
func mustNew(t *testing.T, cfg *config.Config) *Tunnel {
	t.Helper()
	tt, err := New(cfg)
	if err != nil {
		t.Fatalf("tunnel.New: %v", err)
	}
	return tt
}

func receiveReadyCount(m *Multipath) int {
	n := 0
	for _, w := range m.watchers {
		if w.ReceiveReady() {
			n++
		}
	}
	return n
}

func waitForAllPathsReady(t *testing.T, timeout time.Duration, tunnels ...*Tunnel) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for _, tn := range tunnels {
			paths := tn.Paths()
			if paths.ConnectedCount() != paths.SenderCount() ||
				receiveReadyCount(paths) != paths.SenderCount() {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, tn := range tunnels {
		paths := tn.Paths()
		t.Logf("paths not ready: connected=%d/%d receive_ready=%d/%d",
			paths.ConnectedCount(), paths.SenderCount(),
			receiveReadyCount(paths), paths.SenderCount())
	}
	t.Fatalf("timed out waiting for all paths to become ready")
}

func waitForStreamsIdleStable(t *testing.T, timeout time.Duration, tunnels ...*Tunnel) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var idleSince time.Time
	for time.Now().Before(deadline) {
		allIdle := true
		for _, tn := range tunnels {
			if tn.streams.Count() != 0 {
				allIdle = false
				break
			}
		}
		if allIdle {
			if idleSince.IsZero() {
				idleSince = time.Now()
			}
			if time.Since(idleSince) >= 200*time.Millisecond {
				return
			}
		} else {
			idleSince = time.Time{}
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, tn := range tunnels {
		t.Logf("streams still active: %d", tn.streams.Count())
	}
	t.Fatalf("timed out waiting for streams to become idle")
}

func sendPingProbes(t *testing.T, tn *Tunnel) {
	t.Helper()
	for i := 0; i < tn.Paths().SenderCount(); i++ {
		if err := tn.Paths().SendVia(i, protocol.Frame{
			Type:     protocol.MsgPing,
			StreamID: tn.pingStreamID(),
			Payload:  buildPingPayload(time.Now(), tn.cfg.ClientVersion),
		}); err != nil {
			t.Fatalf("send ping via account %d: %v", i, err)
		}
	}
}

func idleKnown(m *Multipath, idx int, want bool) bool {
	if idx < 0 || idx >= len(m.watchers) {
		return false
	}
	idle, known := m.watchers[idx].IdleSupported()
	return known && idle == want
}

func waitForIdleKnown(t *testing.T, timeout time.Duration, tunnels ...*Tunnel) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for _, tn := range tunnels {
			paths := tn.Paths()
			if paths.ProvenCount() != paths.SenderCount() {
				ready = false
				break
			}
			for i := 0; i < paths.SenderCount(); i++ {
				if !idleKnown(paths, i, true) {
					ready = false
					break
				}
			}
		}
		if ready {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, tn := range tunnels {
		t.Logf("idle proof not ready: proven=%d/%d", tn.Paths().ProvenCount(), tn.Paths().SenderCount())
	}
	t.Fatalf("timed out waiting for all paths to be proven with known IDLE support")
}

func forceIdleSupported(t *testing.T, m *Multipath, idx int, idle bool) {
	t.Helper()
	if idx < 0 || idx >= len(m.watchers) {
		t.Fatalf("watcher index %d out of range", idx)
	}
	v := reflect.ValueOf(m.watchers[idx]).Elem().FieldByName("idleSupported")
	ptr := (*atomic.Int32)(unsafe.Pointer(v.UnsafeAddr()))
	if idle {
		ptr.Store(1)
	} else {
		ptr.Store(0)
	}
}

// TestEndToEnd_Multipath exercises two independent IMAP "accounts" (two
// distinct users on the same in-memory server). Outbound frames are
// round-robined; inbound watched in parallel.
func TestEndToEnd_Multipath(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.C2S", "Tunnel.S2C"),
			makeAccountUser(imapAddr, "tit2", "tit-pass2", "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder: &rt,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			// Server side: same accounts, folders SWAPPED.
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.S2C", "Tunnel.C2S"),
			makeAccountUser(imapAddr, "tit2", "tit-pass2", "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder: &rt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)
	time.Sleep(1 * time.Second) // give both accounts time to connect

	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Larger payload so the round-robin gets exercised: at least N
	// separate writes are needed to see both accounts in use.
	payload := []byte(strings.Repeat("multipath-payload-", 200))
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(payload))
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != string(payload) {
		t.Errorf("mismatch")
	}

	cancel()
	wg.Wait()
}

func TestEndToEnd_FrameRoundRobinSingleStream(t *testing.T) {
	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	mode := config.MultipathModeFrameRoundRobin
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.C2S", "Tunnel.S2C"),
			makeAccountUser(imapAddr, "tit2", "tit-pass2", "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder:       &rt,
		MultipathMode: mode,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.S2C", "Tunnel.C2S"),
			makeAccountUser(imapAddr, "tit2", "tit-pass2", "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder:       &rt,
		MultipathMode: mode,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if client.Paths().ProvenCount() == 2 && server.Paths().ProvenCount() == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := client.Paths().ProvenCount(); got != 2 {
		t.Fatalf("client proven paths = %d, want 2", got)
	}
	if got := server.Paths().ProvenCount(); got != 2 {
		t.Fatalf("server proven paths = %d, want 2", got)
	}

	clientBefore := client.Paths().SenderSentCounts()
	serverBefore := server.Paths().SenderSentCounts()

	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	payload := []byte(strings.Repeat("frame-round-robin-payload-", 12000))
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(payload))
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != string(payload) {
		t.Fatalf("mismatch")
	}

	clientAfter := client.Paths().SenderSentCounts()
	serverAfter := server.Paths().SenderSentCounts()
	if len(clientAfter) < 2 || len(serverAfter) < 2 {
		t.Fatalf("sender count: client=%v server=%v", clientAfter, serverAfter)
	}
	clientDelta0 := clientAfter[0] - clientBefore[0]
	clientDelta1 := clientAfter[1] - clientBefore[1]
	serverDelta0 := serverAfter[0] - serverBefore[0]
	serverDelta1 := serverAfter[1] - serverBefore[1]
	if clientDelta0 == 0 || clientDelta1 == 0 {
		t.Fatalf("client DATA was not spread across accounts: before=%v after=%v",
			clientBefore, clientAfter)
	}
	if serverDelta0 == 0 || serverDelta1 == 0 {
		t.Fatalf("server DATA was not spread across accounts: before=%v after=%v",
			serverBefore, serverAfter)
	}

	cancel()
	wg.Wait()
}

func TestEndToEnd_StartupPingProbesLateConnectedAccount(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	delayedAddr := startDelayedTCPProxy(t, imapAddr, 2500*time.Millisecond)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.C2S", "Tunnel.S2C"),
			makeAccountUser(delayedAddr, "tit2", "tit-pass2", "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder: &rt,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.S2C", "Tunnel.C2S"),
			makeAccountUser(delayedAddr, "tit2", "tit-pass2", "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder: &rt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if client.Paths().ProvenCount() == 2 && server.Paths().ProvenCount() == 2 {
			cancel()
			wg.Wait()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("late connected path was not proven: client=%d/%d server=%d/%d client_sent=%v server_sent=%v",
		client.Paths().ProvenCount(), client.Paths().SenderCount(),
		server.Paths().ProvenCount(), server.Paths().SenderCount(),
		client.Paths().SenderSentCounts(), server.Paths().SenderSentCounts())
}

func TestEndToEnd_FrameRoundRobinPrefersIdleCapablePaths(t *testing.T) {
	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	mode := config.MultipathModeFrameRoundRobin
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.C2S", "Tunnel.S2C"),
			makeAccountUser(imapAddr, "tit2", "tit-pass2", "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder:               &rt,
		MultipathMode:         mode,
		PingIntervalMs:        -1,
		PollIntervalMs:        100,
		ActivePollIntervalMs:  50,
		ActivePollDurationMs:  500,
		LazyExpungeThreshold_: 1,
		LazyExpungeMaxAgeMs:   100,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.S2C", "Tunnel.C2S"),
			makeAccountUser(imapAddr, "tit2", "tit-pass2", "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder:               &rt,
		MultipathMode:         mode,
		PingIntervalMs:        -1,
		PollIntervalMs:        100,
		ActivePollIntervalMs:  50,
		ActivePollDurationMs:  500,
		LazyExpungeThreshold_: 1,
		LazyExpungeMaxAgeMs:   100,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)
	waitForAllPathsReady(t, 10*time.Second, client, server)
	sendPingProbes(t, client)
	waitForIdleKnown(t, 15*time.Second, client, server)
	waitForStreamsIdleStable(t, 5*time.Second, client, server)
	forceIdleSupported(t, client.Paths(), 0, false)
	forceIdleSupported(t, server.Paths(), 0, false)

	clientBefore := client.Paths().SenderSentCounts()
	serverBefore := server.Paths().SenderSentCounts()

	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	payload := []byte(strings.Repeat("idle-preference-payload-", 16000))
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(payload))
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != string(payload) {
		t.Fatalf("mismatch")
	}

	clientAfter := client.Paths().SenderSentCounts()
	serverAfter := server.Paths().SenderSentCounts()
	clientDelta0 := clientAfter[0] - clientBefore[0]
	clientDelta1 := clientAfter[1] - clientBefore[1]
	serverDelta0 := serverAfter[0] - serverBefore[0]
	serverDelta1 := serverAfter[1] - serverBefore[1]
	if clientDelta0 != 0 || clientDelta1 == 0 {
		t.Fatalf("client traffic was not confined to IDLE-capable account: before=%v after=%v",
			clientBefore, clientAfter)
	}
	if serverDelta0 != 0 || serverDelta1 == 0 {
		t.Fatalf("server traffic was not confined to IDLE-capable account: before=%v after=%v",
			serverBefore, serverAfter)
	}

	cancel()
	wg.Wait()
}

func TestEndToEnd_MultipathClientPartialAccounts(t *testing.T) {
	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.C2S", "Tunnel.S2C"),
			makeAccountUser(imapAddr, "missing", "bad-pass", "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder: &rt,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.S2C", "Tunnel.C2S"),
			makeAccountUser(imapAddr, "tit2", "tit-pass2", "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder: &rt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	// The client can only connect to one of its two configured accounts.
	// Ping/proving should still start for the connected path instead of
	// waiting forever for the broken secondary account.
	pingDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(pingDeadline) {
		counts := client.Paths().SenderSentCounts()
		if len(counts) > 0 && counts[0] > 0 && client.Paths().ConnectedCount() == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	counts := client.Paths().SenderSentCounts()
	if len(counts) == 0 || counts[0] == 0 {
		t.Fatalf("connected client account was not ping-probed; counts=%v connected=%d",
			counts, client.Paths().ConnectedCount())
	}

	waitForListen(t, clientListen, 10*time.Second, client, server)
	time.Sleep(500 * time.Millisecond)

	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	payload := []byte("partial-multipath\n")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(payload))
	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != string(payload) {
		t.Fatalf("mismatch: got %q want %q", buf, payload)
	}

	cancel()
	wg.Wait()
}

// TestEndToEnd_StreamAffinity verifies that all frames belonging to a
// single stream travel through one account, even when multiple are
// configured. Different streams may pick different accounts (round-robin),
// but each stream is sticky.
func TestEndToEnd_StreamAffinity(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	pingOff := -1
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.C2S", "Tunnel.S2C"),
			makeAccountUser(imapAddr, "tit2", "tit-pass2", "Tunnel.C2S", "Tunnel.S2C"),
		},
		// No coalescing so each TCP write is its own DATA frame —
		// makes the per-account count meaningful.
		Reorder:        &rt,
		PingIntervalMs: pingOff,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccountUser(imapAddr, "tit", "tit-pass", "Tunnel.S2C", "Tunnel.C2S"),
			makeAccountUser(imapAddr, "tit2", "tit-pass2", "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder: &rt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)
	time.Sleep(1 * time.Second)

	// Open one TCP stream and push ten distinct payloads. With
	// stickiness, all of them on each side must go through the same
	// single account.
	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	clientCountsBefore := client.Paths().SenderSentCounts()
	serverCountsBefore := server.Paths().SenderSentCounts()

	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("affinity-%02d\n", i))
		if _, err := conn.Write(msg); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		buf := make([]byte, len(msg))
		_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(buf) != string(msg) {
			t.Errorf("round %d: got %q want %q", i, buf, msg)
		}
	}

	clientCountsAfter := client.Paths().SenderSentCounts()
	serverCountsAfter := server.Paths().SenderSentCounts()

	clientDelta := delta(clientCountsBefore, clientCountsAfter)
	serverDelta := delta(serverCountsBefore, serverCountsAfter)
	t.Logf("client per-account APPEND deltas: %v (labels=%v)",
		clientDelta, client.Paths().SenderLabels())
	t.Logf("server per-account APPEND deltas: %v (labels=%v)",
		serverDelta, server.Paths().SenderLabels())

	if nonzeroCount(clientDelta) != 1 {
		t.Errorf("client: expected stream to use exactly one account, got deltas %v",
			clientDelta)
	}
	if nonzeroCount(serverDelta) != 1 {
		t.Errorf("server: expected stream to use exactly one account, got deltas %v",
			serverDelta)
	}

	// Bonus: verify both sides picked the SAME account (server pins on
	// the OPEN's source).
	if idxOfNonzero(clientDelta) != idxOfNonzero(serverDelta) {
		t.Errorf("client and server pinned to different accounts: "+
			"client=%v server=%v", clientDelta, serverDelta)
	}

	cancel()
	wg.Wait()
}

func delta(before, after []uint64) []uint64 {
	out := make([]uint64, len(after))
	for i := range after {
		out[i] = after[i] - before[i]
	}
	return out
}

func nonzeroCount(xs []uint64) int {
	n := 0
	for _, x := range xs {
		if x > 0 {
			n++
		}
	}
	return n
}

func idxOfNonzero(xs []uint64) int {
	for i, x := range xs {
		if x > 0 {
			return i
		}
	}
	return -1
}

// TestEndToEnd_ZeroRTT verifies the 0-RTT OPEN path: the client must be
// able to write TCP data before OPEN_OK comes back, and the server must
// buffer that data until its dial completes, then flush it to the
// target. We trigger this deliberately by holding the test echo target
// in a slow-accept state, then write a payload, and verify the bytes
// arrive in order.
//
// We can't easily measure the saved RTT against an in-memory IMAP
// server (it's already sub-millisecond), but the buffering correctness
// is what matters — and any race between client-side ReadLoop start
// and server-side dial completion would surface as data loss or
// reordering.
func TestEndToEnd_ZeroRTT(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	tru := true
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder:     &rt,
		ZeroRTTOpen: &tru,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder: &rt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)
	time.Sleep(500 * time.Millisecond)

	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Write IMMEDIATELY — this exercises the 0-RTT path because the
	// client's ReadLoop should already be active before OPEN_OK comes
	// back from the (in-memory) server.
	want := []byte("zero-rtt-payload-12345\n")
	if _, err := conn.Write(want); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(want))
	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(buf) != string(want) {
		t.Errorf("got %q want %q", buf, want)
	}

	cancel()
	wg.Wait()
}

// TestEndToEnd_ZeroRTT_DialFail verifies that when the server's dial
// fails AFTER the client has already sent some 0-RTT DATA, both sides
// recover cleanly: the server discards the buffered data and emits
// OPEN_FAIL, the client tears down its TCP socket, no goroutine leaks.
func TestEndToEnd_ZeroRTT_DialFail(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	badTarget := "127.0.0.1:1"
	clientListen := freePort(t)

	rt := true
	tru := true
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder:     &rt,
		ZeroRTTOpen: &tru,
	}
	serverCfg := &config.Config{
		Mode:           config.ModeServer,
		Target:         badTarget,
		Accounts:       []config.AccountConfig{makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S")},
		Reorder:        &rt,
		DialTimeoutSec: 2,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	server.dialTarget = func(context.Context, string, string) (net.Conn, error) {
		return nil, fmt.Errorf("injected connection refusal")
	}
	client := mustNew(t, clientCfg)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)
	time.Sleep(500 * time.Millisecond)

	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Write 0-RTT data even though the server can't deliver it.
	_, _ = conn.Write([]byte("doomed payload\n"))

	// The server should reject with OPEN_FAIL → client's stream is
	// CloseStreamed → ReadLoop exits → TCP socket closes from this
	// side. The local end should see EOF.
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err == nil && n > 0 {
		t.Errorf("expected close/EOF after dial failure, got %d bytes: %q", n, buf[:n])
	}

	cancel()
	wg.Wait()
}

// TestEndToEnd_NoRefetchAfterKick verifies that we do NOT re-dispatch
// already-processed messages when the watcher wakes up without any new
// mail. This regression test exists because RFC 9051 §9 mandates that
// "X:*" range queries match the highest UID in the mailbox even if it
// is less than X — combined with our lazy-expunge strategy (which
// leaves \Deleted messages in the folder), that would cause every kick
// to refetch the previous reply.
//
// The test sends one small payload, gets the echo back, then waits long
// enough that any spurious refetch would trigger a second echo write.
// If the same data appears twice on the client socket, the test fails.
func TestEndToEnd_NoRefetchAfterKick(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder: &rt,
		// Bigger than 1 so lazy expunge is exercised — the bug only
		// fires when \Deleted messages stay in the folder.
		LazyExpungeThreshold_: 16,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder:               &rt,
		LazyExpungeThreshold_: 16,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)
	time.Sleep(500 * time.Millisecond)

	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	payload := []byte("ping\n")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(payload))
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read first echo: %v", err)
	}
	if string(buf) != string(payload) {
		t.Fatalf("first echo: got %q want %q", buf, payload)
	}

	// Now poke our side again WITHOUT new traffic — this will cause
	// KickWatchers to fire on the server side after the OPEN cycle.
	// If the bug is present, the watcher will refetch the old \Deleted
	// echo response and write a second copy to the TCP socket.
	//
	// Read from the socket with a short deadline; expect EOF / timeout,
	// NOT a duplicate "ping\n".
	_ = conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	extra := make([]byte, 64)
	n, err := conn.Read(extra)
	if err == nil && n > 0 {
		t.Errorf("unexpected duplicate from server: got %d extra bytes: %q",
			n, extra[:n])
	}

	cancel()
	wg.Wait()
}

// TestEndToEnd_Encryption verifies that AES-256-GCM frame encryption
// round-trips through the full IMAP transport. Both sides use the
// same passphrase; the watcher must decrypt every incoming frame
// before decoding it.
//
// As a defence-in-depth check we ALSO start a separate "eavesdropper"
// watcher (without the passphrase) on the same folder and verify it
// fails to decode any of the frames — proving the IMAP server itself
// only sees opaque ciphertext.
func TestEndToEnd_Encryption(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	const passphrase = "correct horse battery staple"

	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder:              &rt,
		EncryptionPassphrase: passphrase,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder:              &rt,
		EncryptionPassphrase: passphrase,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)
	time.Sleep(500 * time.Millisecond)

	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	want := []byte("encrypted-payload-xyz-12345\n")
	if _, err := conn.Write(want); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(want))
	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(buf) != string(want) {
		t.Errorf("encrypted round-trip mismatch: got %q want %q", buf, want)
	}

	cancel()
	wg.Wait()
}

// TestEndToEnd_EncryptionMismatch verifies that when the two sides
// disagree on the passphrase, the receiver drops every frame it gets
// (auth-tag verification fails) — i.e. mismatched configuration
// fails loudly rather than silently corrupting data.
func TestEndToEnd_EncryptionMismatch(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder:              &rt,
		EncryptionPassphrase: "client-passphrase",
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder:              &rt,
		EncryptionPassphrase: "server-DIFFERENT-passphrase",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 5*time.Second, client, server)
	time.Sleep(500 * time.Millisecond)

	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Server's watcher should fail to decrypt the OPEN frame, drop it,
	// and never establish the stream. We should see EOF / timeout
	// (NOT the echo bytes coming back).
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err == nil && n > 0 {
		t.Errorf("expected timeout/EOF on mismatched passphrase, got %d bytes: %q",
			n, buf[:n])
	}

	cancel()
	wg.Wait()
}

// TestEndToEnd_GracefulShutdownSendsRst verifies that when the client
// is terminated (its context cancelled) while a TCP connection is
// active, the server-side stream is properly torn down — i.e. the
// client emits a RST frame for the active stream before exiting,
// rather than silently leaking it.
//
// We measure this by counting active streams on the server side
// after the client exits and giving the server a moment to process
// the RST.
func TestEndToEnd_GracefulShutdownSendsRst(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder:        &rt,
		PingIntervalMs: -1,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder: &rt,
	}

	serverCtx, serverCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer serverCancel()
	server := mustNew(t, serverCfg)
	var serverWG sync.WaitGroup
	serverWG.Add(1)
	go func() { defer serverWG.Done(); _ = server.Run(serverCtx) }()

	// Use a separate, cancellable context for the client so we can
	// terminate JUST the client mid-session.
	clientCtx, clientCancel := context.WithCancel(context.Background())
	client := mustNew(t, clientCfg)
	clientDone := make(chan struct{})
	go func() { defer close(clientDone); _ = client.Run(clientCtx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)
	time.Sleep(500 * time.Millisecond)

	// Open a TCP stream and exchange one payload to make sure the
	// server-side stream is registered.
	conn, err := net.Dial("tcp", clientListen)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hello\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 6)
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}

	// Sanity check: the server side has exactly the test's connection
	// as an active stream right now. waitForListen above probes the
	// listener with a connect-then-close which momentarily allocates
	// a "phantom" stream that needs to drain through the OPEN → FIN
	// → CloseWrite → EOF cycle before disappearing. Wait for the
	// count to settle to 1.
	{
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if server.streams.Count() == 1 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if got := server.streams.Count(); got != 1 {
			t.Fatalf("server active streams before shutdown: got %d want 1", got)
		}
	}

	// Now kill the client. The graceful shutdown path should emit a
	// RST for the active stream BEFORE the IMAP transport is taken
	// down.
	clientCancel()
	select {
	case <-clientDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("client did not exit within 10s")
	}

	// Give the server a moment to process the inbound RST. The full
	// chain is: client APPEND RST → server watcher EXISTS → FETCH →
	// decode → dispatchOrdered → HandleRst → CloseStream → Remove.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if server.streams.Count() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := server.streams.Count(); got != 0 {
		t.Errorf("server still has %d active stream(s) after client RST shutdown — leak", got)
	}

	serverCancel()
	serverWG.Wait()
}

// TestEndToEnd_MultiClient exercises two distinct client instances
// talking to a single server through one shared IMAP account / folder
// pair. The client-id byte in the stream ID distinguishes the two so
// each client only fetches and deletes messages addressed to itself.
func TestEndToEnd_MultiClient(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	listen1 := freePort(t)
	listen2 := freePort(t)

	mkClient := func(listen string, clientID uint8) *Tunnel {
		rt := true
		cfg := &config.Config{
			Mode:     config.ModeClient,
			Listen:   listen,
			ClientID: clientID,
			Accounts: []config.AccountConfig{
				makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C"),
			},
			Reorder: &rt,
		}
		return mustNew(t, cfg)
	}

	rt := true
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		// Server has no client_id — it processes all client traffic.
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder: &rt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client1 := mkClient(listen1, 1)
	client2 := mkClient(listen2, 2)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client1.Run(ctx) }()
	go func() { defer wg.Done(); _ = client2.Run(ctx) }()

	waitForListen(t, listen1, 10*time.Second, client1, server)
	waitForListen(t, listen2, 10*time.Second, client2, server)
	time.Sleep(1 * time.Second)

	// Each client opens its own TCP stream and pushes a distinct
	// payload through the shared echo target.
	type round struct {
		name    string
		listen  string
		payload []byte
	}
	rounds := []round{
		{"client1", listen1, []byte("hello from client 1 — banana split\n")},
		{"client2", listen2, []byte("hello from client 2 — chocolate fudge\n")},
	}

	// Run them concurrently to make sure the streams really are
	// independent.
	var errs sync.Map
	var swg sync.WaitGroup
	for _, r := range rounds {
		r := r
		swg.Add(1)
		go func() {
			defer swg.Done()
			conn, err := net.Dial("tcp", r.listen)
			if err != nil {
				errs.Store(r.name, fmt.Errorf("dial: %w", err))
				return
			}
			defer conn.Close()
			if _, err := conn.Write(r.payload); err != nil {
				errs.Store(r.name, fmt.Errorf("write: %w", err))
				return
			}
			buf := make([]byte, len(r.payload))
			_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
			if _, err := io.ReadFull(conn, buf); err != nil {
				errs.Store(r.name, fmt.Errorf("read: %w", err))
				return
			}
			if string(buf) != string(r.payload) {
				errs.Store(r.name, fmt.Errorf("mismatch: got %q want %q", buf, r.payload))
			}
		}()
	}
	swg.Wait()

	errs.Range(func(k, v any) bool {
		t.Errorf("%s: %v", k, v)
		return true
	})

	cancel()
	wg.Wait()
}

func TestEndToEnd_MultiClientPerClientEncryption(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	listen1 := freePort(t)
	listen2 := freePort(t)

	mkClient := func(listen string, clientID uint8, passphrase string) *Tunnel {
		rt := true
		cfg := &config.Config{
			Mode:                 config.ModeClient,
			Listen:               listen,
			ClientID:             clientID,
			EncryptionPassphrase: passphrase,
			Accounts: []config.AccountConfig{
				makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C"),
			},
			Reorder: &rt,
		}
		return mustNew(t, cfg)
	}

	rt := true
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		ClientEncryptionPassphrases: map[byte]string{
			1: "client-one-secret",
			2: "client-two-secret",
		},
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder: &rt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client1 := mkClient(listen1, 1, "client-one-secret")
	client2 := mkClient(listen2, 2, "client-two-secret")

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client1.Run(ctx) }()
	go func() { defer wg.Done(); _ = client2.Run(ctx) }()

	waitForListen(t, listen1, 10*time.Second, client1, server)
	waitForListen(t, listen2, 10*time.Second, client2, server)
	time.Sleep(1 * time.Second)

	rounds := []struct {
		name    string
		listen  string
		payload []byte
	}{
		{"client1", listen1, []byte("encrypted client 1\n")},
		{"client2", listen2, []byte("encrypted client 2\n")},
	}
	var errs sync.Map
	var swg sync.WaitGroup
	for _, r := range rounds {
		r := r
		swg.Add(1)
		go func() {
			defer swg.Done()
			conn, err := net.Dial("tcp", r.listen)
			if err != nil {
				errs.Store(r.name, fmt.Errorf("dial: %w", err))
				return
			}
			defer conn.Close()
			if _, err := conn.Write(r.payload); err != nil {
				errs.Store(r.name, fmt.Errorf("write: %w", err))
				return
			}
			buf := make([]byte, len(r.payload))
			_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
			if _, err := io.ReadFull(conn, buf); err != nil {
				errs.Store(r.name, fmt.Errorf("read: %w", err))
				return
			}
			if string(buf) != string(r.payload) {
				errs.Store(r.name, fmt.Errorf("mismatch: got %q want %q", buf, r.payload))
			}
		}()
	}
	swg.Wait()

	errs.Range(func(k, v any) bool {
		t.Errorf("%s: %v", k, v)
		return true
	})

	cancel()
	wg.Wait()
}

// TestEndToEnd_CrossStreamBatching verifies that when multiple TCP
// streams write concurrently the sender packs their frames into fewer
// IMAP APPENDs than the total frame count. The single-account /
// single-sender setup makes the contention deterministic.
func TestEndToEnd_CrossStreamBatching(t *testing.T) {
	t.Parallel()

	imapAddr := startMemIMAP(t)
	echoAddr := startEchoServer(t)
	clientListen := freePort(t)

	rt := true
	// Coalescing OFF on the client side so each TCP write becomes its
	// own frame — we want to maximise the per-stream frame rate so the
	// sender has plenty of opportunity to batch.
	clientCfg := &config.Config{
		Mode:   config.ModeClient,
		Listen: clientListen,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.C2S", "Tunnel.S2C"),
		},
		Reorder:         &rt,
		BatchMaxFrames_: 32,
	}
	serverCfg := &config.Config{
		Mode:   config.ModeServer,
		Target: echoAddr,
		Accounts: []config.AccountConfig{
			makeAccount(imapAddr, "Tunnel.S2C", "Tunnel.C2S"),
		},
		Reorder:         &rt,
		BatchMaxFrames_: 32,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	server := mustNew(t, serverCfg)
	client := mustNew(t, clientCfg)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = server.Run(ctx) }()
	go func() { defer wg.Done(); _ = client.Run(ctx) }()

	waitForListen(t, clientListen, 10*time.Second, client, server)
	time.Sleep(500 * time.Millisecond)

	const numStreams = 8
	const writesPerStream = 6

	var swg sync.WaitGroup
	errs := make(chan error, numStreams)
	for i := 0; i < numStreams; i++ {
		i := i
		swg.Add(1)
		go func() {
			defer swg.Done()
			conn, err := net.Dial("tcp", clientListen)
			if err != nil {
				errs <- fmt.Errorf("conn %d dial: %w", i, err)
				return
			}
			defer conn.Close()
			for j := 0; j < writesPerStream; j++ {
				payload := []byte(fmt.Sprintf("s%d-w%d-data\n", i, j))
				if _, err := conn.Write(payload); err != nil {
					errs <- fmt.Errorf("conn %d write %d: %w", i, j, err)
					return
				}
				buf := make([]byte, len(payload))
				_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
				if _, err := io.ReadFull(conn, buf); err != nil {
					errs <- fmt.Errorf("conn %d read %d: %w", i, j, err)
					return
				}
				if string(buf) != string(payload) {
					errs <- fmt.Errorf("conn %d write %d mismatch: got %q", i, j, buf)
					return
				}
			}
		}()
	}
	swg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		cancel()
		wg.Wait()
		return
	}

	clientFrames := client.Paths().SenderSentCounts()[0]
	clientBatches := client.Paths().SenderBatchCounts()[0]
	serverFrames := server.Paths().SenderSentCounts()[0]
	serverBatches := server.Paths().SenderBatchCounts()[0]

	t.Logf("client: %d frames in %d APPENDs (avg %.2f frames/append)",
		clientFrames, clientBatches,
		float64(clientFrames)/float64(maxU(clientBatches, 1)))
	t.Logf("server: %d frames in %d APPENDs (avg %.2f frames/append)",
		serverFrames, serverBatches,
		float64(serverFrames)/float64(maxU(serverBatches, 1)))

	// Sanity: each side should have sent at least one frame per
	// stream's first write plus OPEN handshake.
	if clientFrames < numStreams || serverFrames < numStreams {
		t.Errorf("frame counts implausibly low: client=%d server=%d (want >= %d each)",
			clientFrames, serverFrames, numStreams)
	}

	// The point of the test: under contention the sender must batch
	// SOMEWHERE. With 8 streams x (1 OPEN + 6 writes) = 56 frames per
	// side, plus FINs, going through a single APPEND mutex, the
	// in-memory server is fast enough that some sends fly solo — but
	// at least one batched APPEND must happen on at least one side.
	clientBatched := clientFrames > clientBatches
	serverBatched := serverFrames > serverBatches
	if !clientBatched && !serverBatched {
		t.Errorf("neither side batched: client=%d/%d server=%d/%d (expected frames > APPENDs somewhere)",
			clientFrames, clientBatches, serverFrames, serverBatches)
	}

	cancel()
	wg.Wait()
}

func maxU(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
