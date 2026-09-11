package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/noiseonwires/true-imap-tunnel/internal/config"
	"github.com/noiseonwires/true-imap-tunnel/internal/protocol"
	"github.com/noiseonwires/true-imap-tunnel/internal/streams"
)

func TestRegisterAndSendOpenHandlesImmediateReplyData(t *testing.T) {
	streamID := protocol.MakeStreamID(7, 42)
	banner := []byte("SSH-2.0-test-server\r\n")
	tun := &Tunnel{
		cfg:          &config.Config{Mode: config.ModeClient},
		pendingOpens: make(map[uint32]chan protocol.Frame),
		pendingDials: make(map[uint32]*pendingDial),
	}

	var sent []protocol.Frame
	tun.streams = streams.NewManager(func(f protocol.Frame) error {
		sent = append(sent, f)
		if f.Type == protocol.MsgOpen {
			// Model one watcher fetch containing the OPEN_OK and the target's
			// immediate SSH banner. Both are dispatched before SendFrame returns.
			tun.dispatchOrdered(protocol.Frame{
				Type:     protocol.MsgOpenOK,
				StreamID: streamID,
				SeqID:    1,
			}, "primary")
			tun.dispatchOrdered(protocol.Frame{
				Type:     protocol.MsgData,
				StreamID: streamID,
				SeqID:    2,
				Payload:  banner,
			}, "primary")
		}
		return nil
	})

	responses := make(chan protocol.Frame, 1)
	tun.pendingOpens[streamID] = responses
	local, peer := net.Pipe()
	defer peer.Close()

	s := &streams.Stream{ID: streamID, Conn: local}
	if err := tun.registerAndSendOpen(s); err != nil {
		t.Fatalf("register and send OPEN: %v", err)
	}
	defer tun.streams.CloseStream(s)

	select {
	case resp := <-responses:
		if resp.Type != protocol.MsgOpenOK {
			t.Fatalf("response type = %s, want OPEN_OK", protocol.TypeName(resp.Type))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OPEN_OK")
	}

	got := make([]byte, len(banner))
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(peer, got); err != nil {
		t.Fatalf("read immediate DATA: %v", err)
	}
	if string(got) != string(banner) {
		t.Fatalf("immediate DATA = %q, want %q", got, banner)
	}

	for _, f := range sent {
		if f.Type == protocol.MsgRst {
			t.Fatalf("sent unexpected RST for immediate DATA: %+v", f)
		}
	}
}

func TestDispatchUnknownStreamDataOrFinSendsRst(t *testing.T) {
	for _, typ := range []byte{protocol.MsgData, protocol.MsgFin} {
		t.Run(protocol.TypeName(typ), func(t *testing.T) {
			tun, sent := newDispatchTestTunnel()

			streamID := protocol.MakeStreamID(7, 42)
			tun.dispatchOrdered(protocol.Frame{
				Type:     typ,
				StreamID: streamID,
				SeqID:    3,
				Payload:  []byte("stale"),
			}, "")
			flushTestControls(tun)

			if len(*sent) != 1 {
				t.Fatalf("sent %d frames, want 1 RST: %+v", len(*sent), *sent)
			}
			rst := (*sent)[0]
			if rst.Type != protocol.MsgRst || rst.StreamID != streamID {
				t.Fatalf("sent %+v, want RST for stream %d", rst, streamID)
			}
			if rst.SeqID != 0 {
				t.Fatalf("RST SeqID = %d, want 0", rst.SeqID)
			}
		})
	}
}

func TestHandleIncomingNeverSeenDataWaitsForOpen(t *testing.T) {
	tun, sent := newDispatchTestTunnel()
	tun.streams.Reorder = true

	streamID := protocol.MakeStreamID(7, 42)
	tun.handleIncomingFrame(protocol.Frame{
		Type:     protocol.MsgData,
		StreamID: streamID,
		SeqID:    153,
		Payload:  []byte("late"),
	}, "primary")

	if len(*sent) != 0 {
		t.Fatalf("sent %+v, want no RST because OPEN may still arrive", *sent)
	}
}

func TestHandleIncomingRecentlyClosedHighSeqDataWaitsForReusedOpen(t *testing.T) {
	tun, sent := newDispatchTestTunnel()
	tun.streams.Reorder = true

	streamID := protocol.MakeStreamID(7, 42)
	conn, peer := net.Pipe()
	tun.streams.Register(&streams.Stream{ID: streamID, Conn: conn})
	for seq := uint32(1); seq < 70; seq++ {
		tun.streams.DispatchFrame(protocol.Frame{
			Type:     protocol.MsgData,
			StreamID: streamID,
			SeqID:    seq,
		}, func(protocol.Frame) {})
	}
	tun.streams.CloseStream(tun.streams.Get(streamID))
	_ = peer.Close()

	tun.handleIncomingFrame(protocol.Frame{
		Type:     protocol.MsgData,
		StreamID: streamID,
		SeqID:    153,
		Payload:  []byte("late"),
	}, "primary")

	if len(*sent) != 0 {
		t.Fatalf("sent %+v, want no RST because a reused stream's OPEN may still arrive", *sent)
	}
}

func TestHandleIncomingRecentlyClosedLowSeqWaitsForReusedOpen(t *testing.T) {
	tun, sent := newDispatchTestTunnel()
	tun.streams.Reorder = true

	streamID := protocol.MakeStreamID(7, 42)
	conn, peer := net.Pipe()
	tun.streams.Register(&streams.Stream{ID: streamID, Conn: conn})
	for seq := uint32(1); seq < 70; seq++ {
		tun.streams.DispatchFrame(protocol.Frame{
			Type:     protocol.MsgData,
			StreamID: streamID,
			SeqID:    seq,
		}, func(protocol.Frame) {})
	}
	tun.streams.CloseStream(tun.streams.Get(streamID))
	_ = peer.Close()

	tun.handleIncomingFrame(protocol.Frame{
		Type:     protocol.MsgData,
		StreamID: streamID,
		SeqID:    2,
		Payload:  []byte("new early data"),
	}, "primary")

	if len(*sent) != 0 {
		t.Fatalf("sent %+v, want no RST because a reused stream's OPEN may still arrive", *sent)
	}
}

func TestDispatchPendingDialDataDoesNotSendUnknownRst(t *testing.T) {
	tun, sent := newDispatchTestTunnel()
	streamID := protocol.MakeStreamID(7, 42)
	pd := &pendingDial{done: make(chan struct{})}
	tun.pendingDials[streamID] = pd

	tun.dispatchOrdered(protocol.Frame{
		Type:     protocol.MsgData,
		StreamID: streamID,
		SeqID:    2,
		Payload:  []byte("early"),
	}, "")

	if len(*sent) != 0 {
		t.Fatalf("sent %+v, want no RST while pending dial buffers DATA", *sent)
	}
	if len(pd.buffered) != 1 || pd.buffered[0].kind != protocol.MsgData {
		t.Fatalf("pending buffer = %+v, want one DATA event", pd.buffered)
	}
}

func TestDispatchOpenRejectedDuringShutdown(t *testing.T) {
	tun, sent := newDispatchTestTunnel()
	tun.shuttingDown.Store(true)

	streamID := protocol.MakeStreamID(3, 9)
	tun.dispatchOrdered(protocol.Frame{Type: protocol.MsgOpen, StreamID: streamID}, "acct")
	flushTestControls(tun)

	if len(*sent) != 1 {
		t.Fatalf("sent %d frames, want 1 OPEN_FAIL: %+v", len(*sent), *sent)
	}
	if f := (*sent)[0]; f.Type != protocol.MsgOpenFail || f.StreamID != streamID {
		t.Fatalf("sent %+v, want OPEN_FAIL for stream %d", f, streamID)
	}
	tun.pendingDialsMu.Lock()
	n := len(tun.pendingDials)
	tun.pendingDialsMu.Unlock()
	if n != 0 {
		t.Fatalf("pendingDials = %d, want 0 (no dial started during shutdown)", n)
	}
}

func newDispatchTestTunnel() (*Tunnel, *[]protocol.Frame) {
	var sent []protocol.Frame
	tun := &Tunnel{
		cfg:          &config.Config{Mode: config.ModeServer},
		pendingOpens: make(map[uint32]chan protocol.Frame),
		pendingDials: make(map[uint32]*pendingDial),
	}
	tun.streams = streams.NewManager(func(f protocol.Frame) error {
		sent = append(sent, f)
		return nil
	})
	return tun, &sent
}

func TestDuplicateOpenDuringDialAndLiveResponse(t *testing.T) {
	for _, reorder := range []bool{true, false} {
		ctx, cancel := context.WithCancel(context.Background())
		tun := mustNew(t, &config.Config{Mode: config.ModeServer})
		tun.baseCtx = ctx
		responses := make(chan protocol.Frame, 10)
		tun.streams = streams.NewManager(func(frame protocol.Frame) error { responses <- frame; return nil })
		tun.streams.Reorder = reorder
		tun.streams.OnDuplicateOpen = tun.replayOpen
		tun.streams.OnRemove = tun.retireOpen
		go tun.runControls(ctx)
		started := make(chan struct{})
		release := make(chan struct{})
		local, remote := net.Pipe()
		var dials atomic.Int32
		tun.dialTarget = func(ctx context.Context, _, _ string) (net.Conn, error) {
			if dials.Add(1) == 1 {
				close(started)
			}
			select {
			case <-release:
				return local, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		open := protocol.Frame{Type: protocol.MsgOpen, StreamID: 4, SeqID: 1}
		tun.handleIncomingFrame(open, "")
		<-started
		var duplicateWG sync.WaitGroup
		for count := 0; count < 20; count++ {
			duplicateWG.Add(1)
			go func() { defer duplicateWG.Done(); tun.handleIncomingFrame(open, "") }()
		}
		duplicateWG.Wait()
		close(release)
		first := receiveTestFrame(t, responses)
		if first.Type != protocol.MsgOpenOK || first.SeqID != 1 || dials.Load() != 1 {
			t.Fatalf("reorder=%v first=%+v dials=%d", reorder, first, dials.Load())
		}
		tun.handleIncomingFrame(open, "")
		replayed := receiveTestFrame(t, responses)
		if replayed.Type != first.Type || replayed.SeqID != first.SeqID || dials.Load() != 1 {
			t.Fatalf("replay=%+v dials=%d", replayed, dials.Load())
		}
		_ = tun.streams.SendFrame(protocol.Frame{Type: protocol.MsgData, StreamID: 4})
		if frame := receiveTestFrame(t, responses); frame.SeqID != 2 {
			t.Fatalf("after replay: %+v", frame)
		}
		tun.streams.CloseAll()
		_ = remote.Close()
		tun.dialWG.Wait()
		tun.handleIncomingFrame(open, "")
		if frame := receiveTestFrame(t, responses); frame.Type != protocol.MsgRst {
			t.Fatalf("closed replay: %+v", frame)
		}
		cancel()
		<-tun.controlDone
	}
}

func TestDuplicateOpenAfterDialFailureAndExpiry(t *testing.T) {
	tun := mustNew(t, &config.Config{Mode: config.ModeServer})
	responses := make(chan protocol.Frame, 8)
	tun.streams = streams.NewManager(func(frame protocol.Frame) error { responses <- frame; return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tun.baseCtx = ctx
	go tun.runControls(ctx)
	var dials atomic.Int32
	tun.dialTarget = func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("target unavailable")
	}
	tun.beginPendingOpen(10, "")
	first := receiveTestFrame(t, responses)
	tun.dialWG.Wait()
	tun.beginPendingOpen(10, "")
	replayed := receiveTestFrame(t, responses)
	if dials.Load() != 1 || first.Type != protocol.MsgOpenFail || replayed.SeqID != first.SeqID || string(replayed.Payload) != string(first.Payload) {
		t.Fatalf("first=%+v replay=%+v dials=%d", first, replayed, dials.Load())
	}
	tun.pendingDialsMu.Lock()
	tun.openResults[11] = &openResult{}
	tun.pendingDialsMu.Unlock()
	tun.sweepOpenResults(time.Now().Add(tun.streams.Retention() + time.Second))
	tun.pendingDialsMu.Lock()
	if tun.openResults[10] != nil || tun.openResults[11] == nil {
		t.Error("expiry removed live state or retained closed state")
	}
	tun.pendingDialsMu.Unlock()
	cancel()
	<-tun.controlDone
}

func receiveTestFrame(t *testing.T, frames <-chan protocol.Frame) protocol.Frame {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for frame")
		return protocol.Frame{}
	}
}

func flushTestControls(tun *Tunnel) {
	for _, reply := range tun.controls {
		_ = tun.streams.SendFrame(reply.frame)
	}
	tun.controls = nil
}

func TestControlReplyBurstPreservesProgress(t *testing.T) {
	tun := mustNew(t, &config.Config{Mode: config.ModeServer})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocked := make(chan struct{})
	release := make(chan struct{})
	responses := make(chan protocol.Frame, 101)
	var once sync.Once
	tun.streams = streams.NewManager(func(frame protocol.Frame) error {
		once.Do(func() { close(blocked) })
		select {
		case <-release:
			responses <- frame
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	go tun.runControls(ctx)
	for count := 0; count < 100; count++ {
		tun.dispatchOrdered(protocol.Frame{Type: protocol.MsgPing, Payload: []byte{byte(count)}}, "")
	}
	<-blocked
	local, remote := net.Pipe()
	defer remote.Close()
	tun.streams.Register(&streams.Stream{ID: 7, Conn: local})
	tun.dispatchOrdered(protocol.Frame{Type: protocol.MsgData, StreamID: 7, Payload: []byte("alive")}, "")
	_ = remote.SetReadDeadline(time.Now().Add(time.Second))
	body := make([]byte, 5)
	if _, err := io.ReadFull(remote, body); err != nil || string(body) != "alive" {
		t.Fatalf("data=%q err=%v", body, err)
	}
	close(release)
	for count := 0; count < 100; count++ {
		frame := receiveTestFrame(t, responses)
		if frame.Type != protocol.MsgPong || len(frame.Payload) != 1 || frame.Payload[0] != byte(count) {
			t.Fatalf("reply %d=%+v", count, frame)
		}
	}
	tun.streams.CloseAll()
	cancel()
	<-tun.controlDone
}

func TestFinWaitsForAllAsyncPaths(t *testing.T) {
	paths := NewMultipath(&config.Config{}, nil, nil, nil)
	first := paths.trackData(7)
	second := paths.trackData(7)
	done := make(chan error, 1)
	go func() { done <- paths.Send(protocol.Frame{Type: protocol.MsgFin, StreamID: 7}) }()
	first(nil)
	select {
	case <-done:
		t.Fatal("FIN overtook second path DATA")
	default:
	}
	second(nil)
	select {
	case err := <-done:
		if !errors.Is(err, ErrNoSenders) {
			t.Fatalf("FIN routing after drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FIN did not resume after DATA")
	}
	paths.RemoveRoute(7)
	if len(paths.async) != 0 {
		t.Fatal("closed stream retained async state")
	}
}

func TestPreOpenFlushCancellation(t *testing.T) {
	tun := mustNew(t, &config.Config{Mode: config.ModeServer})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tun.baseCtx = ctx
	responses := make(chan protocol.Frame, 4)
	tun.streams = streams.NewManager(func(frame protocol.Frame) error { responses <- frame; return nil })
	local, remote := net.Pipe()
	defer remote.Close()
	enteredWrite := make(chan struct{})
	tracked := &signalingWriteConn{Conn: local, started: enteredWrite}
	release := make(chan struct{})
	tun.dialTarget = func(ctx context.Context, _, _ string) (net.Conn, error) {
		select {
		case <-release:
			return tracked, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	tun.beginPendingOpen(7, "")
	pending := tun.lookupPendingDial(7)
	tun.dispatchOrdered(protocol.Frame{Type: protocol.MsgData, StreamID: 7, Payload: []byte("blocked write")}, "")
	close(release)
	_ = receiveTestFrame(t, responses)
	select {
	case <-enteredWrite:
	case <-time.After(time.Second):
		t.Fatal("flush not started")
	}
	tun.dispatchOrdered(protocol.Frame{Type: protocol.MsgRst, StreamID: 7}, "")
	select {
	case <-pending.done:
	case <-time.After(time.Second):
		t.Fatal("RST did not interrupt flush")
	}
	if tun.streams.Count() != 0 || tun.lookupPendingDial(7) != nil {
		t.Fatal("RST retained target or pending dial")
	}
}

type signalingWriteConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (conn *signalingWriteConn) Write(payload []byte) (int, error) {
	conn.once.Do(func() { close(conn.started) })
	return conn.Conn.Write(payload)
}

func TestConcurrentPendingDialsRemainCompatible(t *testing.T) {
	tun := mustNew(t, &config.Config{Mode: config.ModeServer})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tun.baseCtx = ctx
	tun.streams = streams.NewManager(func(protocol.Frame) error { return nil })
	started := make(chan struct{}, 128)
	tun.dialTarget = func(ctx context.Context, _, _ string) (net.Conn, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	for id := uint32(1); id <= 128; id++ {
		tun.beginPendingOpen(id, "")
	}
	for count := 0; count < 128; count++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("new pending dial quota")
		}
	}
	cancel()
	tun.dialWG.Wait()
	if len(tun.pendingDials) != 0 {
		t.Fatal("canceled dials retained")
	}
}

func TestOpenTimeoutSemanticsUnchanged(t *testing.T) {
	tun := mustNew(t, &config.Config{Mode: config.ModeClient, OpenTimeoutSec: 1})
	local, remote := net.Pipe()
	defer remote.Close()
	stream := &streams.Stream{ID: 1, Conn: local}
	entered := make(chan struct{})
	release := make(chan struct{})
	tun.streams = streams.NewManager(func(protocol.Frame) error { close(entered); <-release; return nil })
	responses := make(chan protocol.Frame, 1)
	done := make(chan bool, 1)
	go func() {
		if err := tun.registerAndSendOpen(stream); err != nil {
			done <- false
			return
		}
		done <- tun.awaitOpenResponse(context.Background(), 1, responses, stream)
	}()
	<-entered
	select {
	case <-done:
		t.Fatal("OPEN finished while send stalled")
	case <-time.After(tun.cfg.OpenTimeout() + 50*time.Millisecond):
	}
	if tun.streams.Count() != 1 {
		t.Fatal("early total-operation deadline")
	}
	responses <- protocol.Frame{Type: protocol.MsgOpenOK, StreamID: 1}
	close(release)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("delayed send did not preserve response wait")
		}
	case <-time.After(time.Second):
		t.Fatal("OPEN did not resume")
	}
	tun.streams.CloseAll()
}

func TestCancelOpenDuringSend(t *testing.T) {
	tun := mustNew(t, &config.Config{Mode: config.ModeClient})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	local, remote := net.Pipe()
	defer remote.Close()
	entered := make(chan struct{})
	tun.streams = streams.NewManager(func(protocol.Frame) error { close(entered); <-ctx.Done(); return ctx.Err() })
	done := make(chan error, 1)
	go func() { done <- tun.registerAndSendOpen(&streams.Stream{ID: 1, Conn: local}) }()
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("OPEN send did not cancel")
	}
	if tun.streams.Count() != 0 {
		t.Fatal("canceled OPEN retained stream")
	}
}

func TestEndToEnd_ShutdownWithStalledTransport(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	addr := startMemIMAPWithAppendHook(t, func(mailbox string, _ net.Conn) {
		if mailbox == "Tunnel.C2S" {
			once.Do(func() { close(entered) })
			<-release
		}
	})
	defer close(release)
	client := mustNew(t, &config.Config{Mode: config.ModeClient, Listen: freePort(t), PingIntervalMs: -1, GracefulShutdownMs: 50, Accounts: []config.AccountConfig{makeAccount(addr, "Tunnel.C2S", "Tunnel.S2C")}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = client.Run(ctx) }()
	waitForTransport(t, client)
	conn, err := net.Dial("tcp", client.cfg.Listen)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("OPEN APPEND did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop stalled transport")
	}
	if client.streams.Count() != 0 || len(client.pendingOpens) != 0 {
		t.Fatal("shutdown retained streams or pending OPENs")
	}
}

func TestPreOpenBurstDrainsWithoutTruncation(t *testing.T) {
	tun := mustNew(t, &config.Config{Mode: config.ModeServer})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tun.baseCtx = ctx
	tun.streams = streams.NewManager(func(protocol.Frame) error { return nil })
	local, remote := net.Pipe()
	defer remote.Close()
	release := make(chan struct{})
	tun.dialTarget = func(ctx context.Context, _, _ string) (net.Conn, error) {
		select {
		case <-release:
			return local, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	tun.beginPendingOpen(7, "")
	payload := strings.Repeat("p", 1024*1024)
	for offset := 0; offset < len(payload); offset += 32768 {
		tun.dispatchOrdered(protocol.Frame{Type: protocol.MsgData, StreamID: 7, Payload: []byte(payload[offset : offset+32768])}, "")
	}
	other, otherPeer := net.Pipe()
	defer otherPeer.Close()
	tun.streams.Register(&streams.Stream{ID: 8, Conn: other})
	tun.dispatchOrdered(protocol.Frame{Type: protocol.MsgData, StreamID: 8, Payload: []byte("ok")}, "")
	_ = otherPeer.SetReadDeadline(time.Now().Add(time.Second))
	small := make([]byte, 2)
	if _, err := io.ReadFull(otherPeer, small); err != nil || string(small) != "ok" {
		t.Fatalf("other stream: %q %v", small, err)
	}
	close(release)
	_ = remote.SetReadDeadline(time.Now().Add(3 * time.Second))
	body := make([]byte, len(payload))
	if _, err := io.ReadFull(remote, body); err != nil || string(body) != payload {
		t.Fatalf("pre-open burst err=%v", err)
	}
	tun.streams.CloseAll()
	_ = remote.Close()
	tun.dialWG.Wait()
	if tun.lookupPendingDial(7) != nil {
		t.Fatal("completed pre-open burst retained dial")
	}
}
