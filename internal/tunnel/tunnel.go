package tunnel

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/config"
	titcrypto "github.com/true-imap-tunnel/true-imap-tunnel/internal/crypto"
	imappkg "github.com/true-imap-tunnel/true-imap-tunnel/internal/imap"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/protocol"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/streams"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/tlog"
)

const (
	maxPingClientVersionLen = 256

	// In one-shot ping mode (ping_interval_ms: 0), keep probing during startup
	// so slower accounts that connect after the first path can still become
	// proven and enter normal multipath routing.
	startupPingProbeInterval = time.Second
	startupPingProbeWindow   = 30 * time.Second
)

// Tunnel is the top-level glue: a multipath IMAP transport, a stream
// manager, and either a TCP listener (client mode) or a target dialer
// (server mode).
type Tunnel struct {
	cfg     *config.Config
	streams *streams.Manager
	paths   *Multipath

	// pendingOpens maps an outbound stream ID to the channel that
	// delivers the OPEN_OK / OPEN_FAIL response. Client mode only.
	pendingMu    sync.Mutex
	pendingOpens map[uint32]chan protocol.Frame

	// pendingDials tracks server-side streams whose target dial is
	// still in flight. DATA/FIN/RST frames that arrive for a stream
	// before its dial completes are buffered here so we can flush them
	// to the target socket once the dial succeeds — this is what makes
	// 0-RTT OPEN work end to end.
	pendingDialsMu sync.Mutex
	pendingDials   map[uint32]*pendingDial
}

// pendingDial is the server-side bookkeeping for an in-flight OPEN.
//
// While the dial is running, DATA / FIN / RST frames for the same
// stream are buffered here in the order they arrived. handleOpen, on
// dial success, drains the buffer to the target socket and atomically
// marks the dial as `drained`. After that point, dispatchOrdered for
// the same stream routes frames through streams.HandleData/Fin/Rst
// directly — but only AFTER checking pd.drained, never bypassing the
// buffer while it still has un-flushed events.
type pendingDial struct {
	mu       sync.Mutex
	buffered []bufferedEvent
	drained  bool // true after handleOpen has flushed every queued event
	rstRecv  bool // peer sent RST before dial finished
	cancel   context.CancelFunc
	done     chan struct{} // closed when the dial goroutine exits
}

// bufferedEvent captures a DATA/FIN/RST frame that arrived while a
// stream's dial was still in flight. They are replayed to the target
// socket in receipt order once the dial succeeds.
type bufferedEvent struct {
	kind    byte // protocol.MsgData / MsgFin / MsgRst
	payload []byte
}

// New constructs a Tunnel. Returns an error if encryption configuration
// is invalid.
func New(cfg *config.Config) (*Tunnel, error) {
	keys, err := titcrypto.NewKeyRing(cfg.EncryptionPassphrase, cfg.ClientEncryptionPassphrases)
	if err != nil {
		return nil, fmt.Errorf("encryption setup: %w", err)
	}
	if keys.Enabled() {
		if keys.ClientKeys() > 0 {
			tlog.Infof("encryption: AES-256-GCM enabled (%d client key(s), %dB overhead/frame)",
				keys.ClientKeys(), keys.Overhead())
		} else {
			tlog.Infof("encryption: AES-256-GCM enabled (passphrase-derived key, %dB overhead/frame)",
				keys.Overhead())
		}
	}

	t := &Tunnel{
		cfg:          cfg,
		pendingOpens: make(map[uint32]chan protocol.Frame),
		pendingDials: make(map[uint32]*pendingDial),
	}

	t.streams = streams.NewManager(t.sendFrame)
	t.streams.Reorder = cfg.ReorderEnabled()
	t.streams.InboundQueueSize = cfg.InboundQueueSize()
	t.streams.OutboundQueueWait = cfg.InboundQueueWait()
	t.streams.OnReorderReset = func(streamID uint32) {
		label := t.paths.RouteLabel(streamID)
		rst := protocol.Frame{Type: protocol.MsgRst, StreamID: streamID}
		var err error
		if label != "" {
			err = t.paths.SendViaLabel(label, rst)
		} else {
			err = t.paths.Send(rst)
		}
		if err != nil {
			tlog.Debugf("reorder reset RST send failed stream=%d: %v", streamID, err)
		}
	}

	// Multi-client filter: when ClientID is set on a client, only
	// messages whose stream is tagged with that client-id are processed
	// and deleted. The server processes every frame it sees.
	var filter imappkg.FrameFilter
	if cfg.Mode == config.ModeClient && cfg.ClientID != 0 {
		ownID := cfg.ClientID
		filter = func(f protocol.Frame) bool {
			if f.StreamID == 0 {
				return true
			}
			return protocol.StreamClientID(f.StreamID) == ownID
		}
	}

	t.paths = NewMultipath(cfg, t.handleIncomingFrame, filter, keys)
	t.paths.OnAsyncSendError = t.handleAsyncSendError
	// Drop the route entry when a stream is removed, so the route table
	// doesn't grow without bound.
	t.streams.OnRemove = func(streamID uint32) {
		tlog.Infof("stream closed stream=%d client_id=%d mode=%s account=%q active_streams=%d",
			streamID, protocol.StreamClientID(streamID), cfg.Mode, t.paths.RouteLabel(streamID), t.streams.Count())
		t.paths.CancelStream(streamID)
		t.paths.RemoveRoute(streamID)
		// Also wake any pending OPEN goroutine that was waiting on this
		// stream ID — there shouldn't be one (since pending entries are
		// keyed before the stream is registered), but it's defensive.
		t.pendingMu.Lock()
		if ch, ok := t.pendingOpens[streamID]; ok {
			delete(t.pendingOpens, streamID)
			close(ch)
		}
		t.pendingMu.Unlock()
	}
	return t, nil
}

// Paths returns the multipath instance, for diagnostics and tests.
func (t *Tunnel) Paths() *Multipath { return t.paths }

// sendFrame is the streams.SendFunc — routes outbound frames through the
// multipath transport.
func (t *Tunnel) sendFrame(f protocol.Frame) error {
	return t.paths.Send(f)
}

func (t *Tunnel) handleAsyncSendError(f protocol.Frame, err error) {
	if f.Type != protocol.MsgData || f.StreamID == 0 {
		return
	}
	s := t.streams.Get(f.StreamID)
	if s == nil {
		return
	}
	tlog.Warnf("async DATA send failed stream=%d client_id=%d: %v; resetting stream",
		f.StreamID, protocol.StreamClientID(f.StreamID), err)
	if rstErr := t.streams.SendFrame(protocol.Frame{Type: protocol.MsgRst, StreamID: f.StreamID}); rstErr != nil {
		tlog.Debugf("async DATA reset send failed stream=%d: %v", f.StreamID, rstErr)
	}
	t.streams.CloseStream(s)
}

// Run executes the configured mode until ctx is cancelled. On exit it
// performs a best-effort graceful shutdown:
//
//  1. snapshot the IDs of every active stream;
//  2. emit a RST frame for each so the peer can release its target
//     sockets and stream-manager bookkeeping;
//  3. only THEN tear down the multipath senders / watchers;
//  4. close local TCP sockets and wait for all goroutines.
//
// The whole sequence is bounded by cfg.GracefulShutdown() — if the
// IMAP server is unreachable we don't want to hang forever.
//
// To make step 3 possible we give the multipath its own context that
// outlives the caller's ctx by exactly the shutdown window. Otherwise
// the senders would already be tearing down by the time we try to
// dispatch the RST frames.
func (t *Tunnel) Run(ctx context.Context) error {
	pathsCtx, cancelPaths := context.WithCancel(context.Background())
	defer cancelPaths()
	t.paths.Start(pathsCtx)

	// Client-only: periodic end-to-end RTT probe. Disabled in server
	// mode (servers only respond to pings, they don't initiate them).
	if t.cfg.Mode == config.ModeClient && t.cfg.PingEnabled() {
		go t.runPingLoop(ctx)
	}

	var err error
	switch t.cfg.Mode {
	case config.ModeClient:
		err = t.runClient(ctx)
	case config.ModeServer:
		err = t.runServer(ctx)
	default:
		err = fmt.Errorf("unsupported mode %q", t.cfg.Mode)
	}

	// Graceful shutdown: tell the peer about every still-open stream
	// before we tear down the transport.
	t.gracefulShutdown()

	// Now stop the multipath and wait for it.
	cancelPaths()
	t.paths.Wait()

	// Local TCP sockets last — by this point all peer state has been
	// notified and the transport is gone.
	t.streams.CloseAll()
	return err
}

// gracefulShutdown emits a RST frame for every still-active stream so
// the peer can release its bookkeeping (server-side: target socket +
// stream-manager entry). Bounded by cfg.GracefulShutdown().
//
// This runs while the multipath is still alive, so RSTs travel via
// the normal Send path. If the deadline expires partway through we
// simply stop — the peer will eventually time out the orphans, but
// most of the streams will have been cleaned up cleanly.
func (t *Tunnel) gracefulShutdown() {
	ids := t.streams.ActiveIDs()
	if len(ids) == 0 {
		return
	}
	deadline := t.cfg.GracefulShutdown()
	tlog.Infof("graceful shutdown: emitting RST for %d active stream(s) (deadline %v)",
		len(ids), deadline)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, id := range ids {
			if err := t.streams.SendFrame(protocol.Frame{
				Type:     protocol.MsgRst,
				StreamID: id,
			}); err != nil {
				tlog.Debugf("graceful shutdown: RST stream=%d failed: %v",
					id, err)
				// Don't break — try the rest; transient sender
				// failures shouldn't deny other streams a clean
				// teardown.
			}
		}
	}()
	select {
	case <-done:
		tlog.Debugf("graceful shutdown: RST burst complete")
	case <-time.After(deadline):
		tlog.Warnf("graceful shutdown: deadline %v expired, %d stream(s) may not have received RST",
			deadline, len(ids))
	}
}

// runPingLoop sends Pings after the IMAP transport has at least one ready path.
// In one-shot mode it keeps probing unproven paths for a short startup window
// so accounts that connect a few seconds later can still become eligible for
// multipath routing. In periodic mode it keeps re-sending on a fixed cadence.
// dispatchOrdered logs the round-trip latency when the matching Pong returns.
// The first 8 payload bytes are the big-endian UnixNano send timestamp,
// optionally followed by a UTF-8 client version string for server-side
// diagnostics.
func (t *Tunnel) runPingLoop(ctx context.Context) {
	// Wait for at least one sender and one receive-ready watcher before
	// probing. Sending before the watcher has established its UID baseline can
	// race the first PONG into the startup snapshot and make it look stale.
	for !t.clientTransportReady() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}

	// sendConnected sends a Ping through every locally ready sender so each path
	// independently gets proven by its Pong. Without this, the first
	// account to respond would become "proven" and the proven-filter
	// would starve all other accounts forever.
	sendConnected := func(onlyUnproven bool) int {
		sent := 0
		for i := 0; i < t.paths.SenderCount(); i++ {
			if !t.paths.SenderConnected(i) || !t.paths.ReceiveReady(i) {
				continue
			}
			if onlyUnproven && t.paths.PathProven(i) {
				continue
			}
			if err := t.paths.SendVia(i, protocol.Frame{
				Type:     protocol.MsgPing,
				StreamID: t.pingStreamID(),
				Payload:  buildPingPayload(time.Now(), t.cfg.ClientVersion),
			}); err != nil {
				tlog.Debugf("ping send via account %d failed: %v", i, err)
			} else {
				sent++
			}
		}
		return sent
	}

	// Always probe once on connect — that's the user's first signal
	// that the round trip is actually working.
	if t.cfg.PingPeriodic() {
		tlog.Infof("ping loop: one probe now, then every %v",
			t.cfg.PingInterval())
	} else {
		tlog.Infof("ping loop: startup probes until all paths are proven or %v elapses (set ping_interval_ms > 0 for repeats)",
			startupPingProbeWindow)
	}
	sendConnected(false)
	if !t.cfg.PingPeriodic() {
		ticker := time.NewTicker(startupPingProbeInterval)
		defer ticker.Stop()
		deadline := time.NewTimer(startupPingProbeWindow)
		defer deadline.Stop()
		for t.paths.ProvenCount() < t.paths.SenderCount() {
			select {
			case <-ctx.Done():
				return
			case <-deadline.C:
				if proven, total := t.paths.ProvenCount(), t.paths.SenderCount(); proven < total {
					tlog.Infof("ping loop: startup probe window ended with %d/%d proven path(s)",
						proven, total)
				}
				return
			case <-ticker.C:
				sendConnected(true)
			}
		}
		return
	}

	ticker := time.NewTicker(t.cfg.PingInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendConnected(false)
		}
	}
}

// handleIncomingFrame is invoked by every watcher for each frame decoded
// from an incoming IMAP message. It dispatches to mode-specific logic
// after running the frame through the reorder buffer.
//
// The source account label is carried through to dispatchOrdered via a
// per-call closure so that server-mode can pin a stream's outbound
// route to the same account that delivered its OPEN.
func (t *Tunnel) handleIncomingFrame(f protocol.Frame, source string) {
	t.streams.DispatchFrame(f, func(of protocol.Frame) {
		t.dispatchOrdered(of, source)
	})
}

// dispatchOrdered is the in-order frame handler. Called sequentially per
// stream by the streams.Manager reorder logic.
//
// source identifies the account label that delivered the triggering
// frame. For frames drained from the reorder buffer, source reflects the
// account that delivered the most recently-arrived frame, not
// necessarily the original frame's source — that distinction matters
// only for OPEN (which is always SeqID=1 and therefore never buffered
// before its own delivery).
func (t *Tunnel) dispatchOrdered(f protocol.Frame, source string) {
	switch f.Type {
	case protocol.MsgOpen:
		// Server-mode only: pin outbound route to the OPEN's source
		// account, register a pending-dial entry, and kick off the
		// dial in the background. DATA/FIN/RST that arrive before the
		// dial completes are buffered on the pendingDial.
		if t.cfg.Mode == config.ModeServer {
			if source != "" {
				t.paths.PinByLabel(f.StreamID, source)
			}
			t.beginPendingOpen(f.StreamID)
		} else {
			tlog.Warnf("client received unexpected OPEN stream=%d", f.StreamID)
		}

	case protocol.MsgOpenOK, protocol.MsgOpenFail:
		// Client-mode only: wake the goroutine that sent OPEN.
		t.pendingMu.Lock()
		ch, ok := t.pendingOpens[f.StreamID]
		t.pendingMu.Unlock()
		if ok {
			// Non-blocking — channel is buffered.
			select {
			case ch <- f:
			default:
			}
		} else {
			tlog.Debugf("%s for unknown stream=%d",
				protocol.TypeName(f.Type), f.StreamID)
		}

	case protocol.MsgData:
		// CRITICAL: consult pendingDial FIRST. If a dial is still in
		// progress, the buffer hasn't been drained yet, and routing
		// this frame through streams.HandleData would cause it to be
		// written to the target ahead of the earlier (still-buffered)
		// frames. Once handleOpen flips pd.drained, this code path
		// falls through to HandleData and behaves normally.
		if pd := t.lookupPendingDial(f.StreamID); pd != nil {
			pd.mu.Lock()
			if !pd.drained {
				cp := make([]byte, len(f.Payload))
				copy(cp, f.Payload)
				pd.buffered = append(pd.buffered,
					bufferedEvent{kind: protocol.MsgData, payload: cp})
				pd.mu.Unlock()
				return
			}
			pd.mu.Unlock()
		}
		if t.resetUnknownStream(f) {
			return
		}
		t.streams.HandleData(f.StreamID, f.Payload)

	case protocol.MsgFin:
		tlog.Debugf("FIN stream=%d seq=%d", f.StreamID, f.SeqID)
		if pd := t.lookupPendingDial(f.StreamID); pd != nil {
			pd.mu.Lock()
			if !pd.drained {
				pd.buffered = append(pd.buffered,
					bufferedEvent{kind: protocol.MsgFin})
				pd.mu.Unlock()
				return
			}
			pd.mu.Unlock()
		}
		if t.resetUnknownStream(f) {
			return
		}
		t.streams.HandleFin(f.StreamID)

	case protocol.MsgRst:
		tlog.Debugf("RST stream=%d seq=%d", f.StreamID, f.SeqID)
		if pd := t.lookupPendingDial(f.StreamID); pd != nil {
			pd.mu.Lock()
			cancel := pd.cancel
			if !pd.drained {
				pd.rstRecv = true
				pd.buffered = append(pd.buffered,
					bufferedEvent{kind: protocol.MsgRst})
				pd.mu.Unlock()
				if cancel != nil {
					cancel()
				}
				return
			}
			pd.mu.Unlock()
		}
		t.streams.HandleRst(f.StreamID)

	case protocol.MsgPing:
		// Echo back as Pong with the same payload (which carries the
		// sender's wall-clock send time). Route the reply through the
		// same account the Ping arrived on so the sender's watcher for
		// that account receives the Pong and marks the path as proven.
		if t.cfg.Mode == config.ModeServer {
			if clientVersion := pingClientVersion(f.Payload); clientVersion != "" {
				tlog.Infof("ping received via %q payload_len=%d client_version=%q",
					source, len(f.Payload), clientVersion)
			} else {
				tlog.Infof("ping received via %q payload_len=%d",
					source, len(f.Payload))
			}
		}
		if len(f.Payload) > 0 {
			payload := make([]byte, len(f.Payload))
			copy(payload, f.Payload)
			pong := protocol.Frame{
				Type:     protocol.MsgPong,
				StreamID: f.StreamID,
				Payload:  payload,
			}
			if err := t.paths.SendViaLabel(source, pong); err != nil {
				if t.cfg.Mode == config.ModeServer {
					tlog.Warnf("pong send failed via %q: %v", source, err)
				} else {
					tlog.Debugf("ping echo failed: %v", err)
				}
			} else if t.cfg.Mode == config.ModeServer {
				tlog.Infof("pong sent via %q payload_len=%d", source, len(payload))
			}
		}

	case protocol.MsgPong:
		// Decode the original send timestamp (UnixNano, big-endian) and
		// log the round-trip latency.
		if len(f.Payload) >= 8 {
			sentNS := int64(binary.BigEndian.Uint64(f.Payload[:8]))
			rtt := time.Since(time.Unix(0, sentNS))
			tlog.Infof("rtt=%v via %q",
				rtt.Round(time.Millisecond), source)
		}

	default:
		tlog.Warnf("unknown frame type=%s stream=%d", protocol.TypeName(f.Type), f.StreamID)
	}
}

func (t *Tunnel) pingStreamID() uint32 {
	if t.cfg.ClientID == 0 {
		return 0
	}
	return protocol.MakeStreamID(t.cfg.ClientID, 0)
}

func (t *Tunnel) resetUnknownStream(f protocol.Frame) bool {
	if f.StreamID == 0 || t.streams.Get(f.StreamID) != nil {
		return false
	}
	tlog.Warnf("%s for unknown stream=%d client_id=%d; sending RST",
		protocol.TypeName(f.Type), f.StreamID, protocol.StreamClientID(f.StreamID))
	if err := t.streams.SendFrame(protocol.Frame{Type: protocol.MsgRst, StreamID: f.StreamID}); err != nil {
		tlog.Debugf("RST for unknown stream=%d failed: %v", f.StreamID, err)
	}
	return true
}

func buildPingPayload(now time.Time, clientVersion string) []byte {
	clientVersion = strings.TrimSpace(clientVersion)
	if len(clientVersion) > maxPingClientVersionLen {
		clientVersion = clientVersion[:maxPingClientVersionLen]
	}
	payload := make([]byte, 8+len(clientVersion))
	binary.BigEndian.PutUint64(payload[:8], uint64(now.UnixNano()))
	copy(payload[8:], clientVersion)
	return payload
}

func pingClientVersion(payload []byte) string {
	if len(payload) <= 8 {
		return ""
	}
	return strings.TrimSpace(string(payload[8:]))
}

// beginPendingOpen registers a pendingDial entry for streamID and
// launches the dial goroutine. Returns immediately so the watcher's
// dispatch loop can deliver subsequent DATA/FIN/RST frames.
func (t *Tunnel) beginPendingOpen(streamID uint32) {
	pd := &pendingDial{done: make(chan struct{})}
	t.pendingDialsMu.Lock()
	t.pendingDials[streamID] = pd
	t.pendingDialsMu.Unlock()
	go t.handleOpen(streamID, pd)
}

// lookupPendingDial returns the pendingDial for streamID, or nil.
func (t *Tunnel) lookupPendingDial(streamID uint32) *pendingDial {
	t.pendingDialsMu.Lock()
	defer t.pendingDialsMu.Unlock()
	return t.pendingDials[streamID]
}

// removePendingDial drops the bookkeeping entry. Idempotent.
func (t *Tunnel) removePendingDial(streamID uint32) {
	t.pendingDialsMu.Lock()
	delete(t.pendingDials, streamID)
	t.pendingDialsMu.Unlock()
}

// --- Client mode ---

// runClient listens on the configured TCP address and creates a new stream
// for each accepted connection.
func (t *Tunnel) runClient(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", t.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", t.cfg.Listen, err)
	}
	tlog.Infof("client listening on %s (forward via %d IMAP account(s))",
		t.cfg.Listen, t.paths.SenderCount())

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			tlog.Warnf("accept error: %v", err)
			continue
		}
		go t.handleAccept(ctx, conn)
	}
}

// handleAccept allocates a stream ID, sends OPEN, and starts the read
// loop. When zero-RTT OPEN is enabled the ReadLoop starts
// immediately in parallel with the OPEN_OK wait, so the first user-data
// byte can begin its trip to the server without paying an extra
// echo-RTT. The OPEN_OK / OPEN_FAIL response is watched by a background
// goroutine that tears the TCP socket down if the dial failed.
func (t *Tunnel) handleAccept(ctx context.Context, conn net.Conn) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	if !t.waitClientTransportReady(ctx, t.cfg.OpenTimeout()) {
		tlog.Warnf("closing TCP from %s: IMAP transport not ready within %v",
			conn.RemoteAddr(), t.cfg.OpenTimeout())
		_ = conn.Close()
		return
	}

	// Stamp the configured ClientID into the top byte of the stream ID
	// so the server can route the response back to this client and
	// other clients sharing the same folder pair leave it alone.
	localID := t.streams.NextID() & protocol.StreamLocalIDMask
	streamID := protocol.MakeStreamID(t.cfg.ClientID, localID)

	// Pick an account for this stream up front. All outbound frames for
	// the stream will be routed through this account, so latency stays
	// consistent and the reorder buffer rarely fires.
	t.paths.AllocRoute(streamID)
	zeroRTT := t.cfg.ZeroRTTOpenEnabled()
	tlog.Infof("new TCP from %s, allocated stream=%d (client_id=%d) via account=%q zero_rtt=%v",
		conn.RemoteAddr(), streamID, t.cfg.ClientID,
		t.paths.RouteLabel(streamID), zeroRTT)

	ch := make(chan protocol.Frame, 1)
	t.pendingMu.Lock()
	t.pendingOpens[streamID] = ch
	t.pendingMu.Unlock()
	cleanupPending := func() {
		t.pendingMu.Lock()
		delete(t.pendingOpens, streamID)
		t.pendingMu.Unlock()
	}
	defer cleanupPending()

	if err := t.streams.SendFrame(protocol.Frame{Type: protocol.MsgOpen, StreamID: streamID}); err != nil {
		tlog.Warnf("send OPEN failed stream=%d client_id=%d: %v",
			streamID, protocol.StreamClientID(streamID), err)
		_ = conn.Close()
		t.paths.RemoveRoute(streamID)
		return
	}

	s := &streams.Stream{ID: streamID, Conn: conn}

	if !zeroRTT {
		// Classic handshake: block until OPEN_OK/FAIL or timeout.
		if !t.awaitOpenResponse(ctx, streamID, ch, s) {
			return
		}
		t.streams.Register(s)
		t.streams.ReadLoop(s)
		t.streams.CloseStream(s)
		return
	}

	// 0-RTT path: register and start ReadLoop NOW; a background
	// goroutine watches the channel and closes the stream if the
	// server replies OPEN_FAIL.
	t.streams.Register(s)
	go func() {
		select {
		case resp, ok := <-ch:
			if !ok {
				// pendingOpens entry was cleared by OnRemove — the
				// stream is already torn down; nothing to do.
				return
			}
			if resp.Type == protocol.MsgOpenFail {
				tlog.Infof("stream rejected stream=%d client_id=%d reason=%s (0-RTT data discarded by server)",
					streamID, protocol.StreamClientID(streamID), string(resp.Payload))
				t.streams.CloseStream(s)
				return
			}
			tlog.Infof("stream opened stream=%d client_id=%d remote=%s",
				streamID, protocol.StreamClientID(streamID), conn.RemoteAddr())
		case <-time.After(t.cfg.OpenTimeout()):
			tlog.Warnf("OPEN timeout stream=%d client_id=%d (0-RTT, sending RST)",
				streamID, protocol.StreamClientID(streamID))
			_ = t.streams.SendFrame(protocol.Frame{Type: protocol.MsgRst, StreamID: streamID})
			t.streams.CloseStream(s)
		case <-ctx.Done():
			t.streams.CloseStream(s)
		}
	}()
	t.streams.ReadLoop(s)
	t.streams.CloseStream(s)
}

func (t *Tunnel) clientTransportReady() bool {
	return t.paths.AnyConnected() && t.paths.AnyReceiveReady()
}

func (t *Tunnel) waitClientTransportReady(ctx context.Context, timeout time.Duration) bool {
	if t.clientTransportReady() {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return t.clientTransportReady()
		case <-ticker.C:
			if t.clientTransportReady() {
				return true
			}
		}
	}
}

// awaitOpenResponse blocks until the server replies OPEN_OK / OPEN_FAIL,
// the open-timeout fires, or ctx is cancelled. Returns true when the
// stream is ready to use, false otherwise (and the conn is closed).
func (t *Tunnel) awaitOpenResponse(ctx context.Context, streamID uint32, ch chan protocol.Frame, s *streams.Stream) bool {
	select {
	case resp, ok := <-ch:
		if !ok {
			_ = s.Conn.Close()
			return false
		}
		if resp.Type == protocol.MsgOpenFail {
			tlog.Infof("stream rejected stream=%d client_id=%d reason=%s",
				streamID, protocol.StreamClientID(streamID), string(resp.Payload))
			_ = s.Conn.Close()
			t.paths.RemoveRoute(streamID)
			return false
		}
		tlog.Infof("stream opened stream=%d client_id=%d remote=%s",
			streamID, protocol.StreamClientID(streamID), s.Conn.RemoteAddr())
		return true
	case <-time.After(t.cfg.OpenTimeout()):
		tlog.Warnf("OPEN timeout stream=%d client_id=%d",
			streamID, protocol.StreamClientID(streamID))
		_ = t.streams.SendFrame(protocol.Frame{Type: protocol.MsgRst, StreamID: streamID})
		_ = s.Conn.Close()
		t.paths.RemoveRoute(streamID)
		return false
	case <-ctx.Done():
		_ = s.Conn.Close()
		return false
	}
}

// --- Server mode ---

// runServer waits for OPEN frames and dials the configured target for
// each one. The dispatch is fanned out via dispatchOrdered → handleOpen
// (in a goroutine).
func (t *Tunnel) runServer(ctx context.Context) error {
	tlog.Infof("server forwarding to %s (via %d IMAP account(s))",
		t.cfg.Target, t.paths.SenderCount())
	<-ctx.Done()
	return nil
}

// handleOpen dials the configured target, sends OPEN_OK or OPEN_FAIL,
// drains any DATA/FIN/RST events that were buffered while the dial was
// in flight (the 0-RTT path), and starts the stream's read loop.
//
// The stream's outbound route has already been pinned to the OPEN's
// source account by dispatchOrdered.
//
// Race-safety: the drain runs as a loop that takes a snapshot of
// pd.buffered under pd.mu, releases the lock to write to the socket,
// then re-acquires the lock. Only when buffered is empty does it flip
// pd.drained = true. dispatchOrdered respects this flag — it can
// either append a fresh event to pd.buffered (which our next loop
// iteration will catch) or, once drained=true, route through the real
// stream. There is no window in which a fresh DATA frame can overtake
// a buffered one.
func (t *Tunnel) handleOpen(streamID uint32, pd *pendingDial) {
	defer close(pd.done)
	defer t.removePendingDial(streamID)

	dialCtx, cancel := context.WithTimeout(context.Background(), t.cfg.DialTimeout())
	pd.mu.Lock()
	pd.cancel = cancel
	earlyRst := pd.rstRecv
	pd.mu.Unlock()
	defer cancel()

	if earlyRst {
		// Peer RST'd before we'd even started — nothing to do.
		t.paths.RemoveRoute(streamID)
		return
	}

	d := net.Dialer{}
	conn, err := d.DialContext(dialCtx, "tcp", t.cfg.Target)
	if err != nil {
		tlog.Warnf("dial target %s failed for stream=%d client_id=%d: %v",
			t.cfg.Target, streamID, protocol.StreamClientID(streamID), err)
		_ = t.streams.SendFrame(protocol.Frame{
			Type:     protocol.MsgOpenFail,
			StreamID: streamID,
			Payload:  []byte(err.Error()),
		})
		t.paths.RemoveRoute(streamID)
		return
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	// Promote the pending dial to a real stream. dispatchOrdered for
	// subsequent frames now sees the registered stream via streams.Get,
	// but is required (by its own logic) to check pd.drained FIRST and
	// keep buffering until we set drained=true below.
	s := &streams.Stream{ID: streamID, Conn: conn}
	t.streams.Register(s)

	if err := t.streams.SendFrame(protocol.Frame{Type: protocol.MsgOpenOK, StreamID: streamID}); err != nil {
		tlog.Warnf("send OPEN_OK failed stream=%d client_id=%d: %v",
			streamID, protocol.StreamClientID(streamID), err)
		t.streams.CloseStream(s)
		return
	}
	tlog.Infof("stream opened stream=%d client_id=%d target=%s account=%q",
		streamID, protocol.StreamClientID(streamID), t.cfg.Target, t.paths.RouteLabel(streamID))

	// Drain loop. Repeats until we see an empty buffer while holding
	// pd.mu — at that exact moment we flip drained=true under the same
	// lock, so no new event can be inserted between the check and the
	// flip.
	totalBuffered := 0
	abort := false
	for !abort {
		pd.mu.Lock()
		if len(pd.buffered) == 0 {
			pd.drained = true
			pd.mu.Unlock()
			break
		}
		chunk := pd.buffered
		pd.buffered = nil
		pd.mu.Unlock()

		for _, ev := range chunk {
			totalBuffered++
			switch ev.kind {
			case protocol.MsgData:
				if _, werr := conn.Write(ev.payload); werr != nil {
					tlog.Warnf("flush buffered DATA to target failed stream=%d: %v",
						streamID, werr)
					abort = true
				}
			case protocol.MsgFin:
				if tc, ok := conn.(*net.TCPConn); ok {
					_ = tc.CloseWrite()
				}
			case protocol.MsgRst:
				abort = true
			}
			if abort {
				break
			}
		}
	}
	if abort {
		t.streams.CloseStream(s)
		return
	}
	if totalBuffered > 0 {
		tlog.Debugf("stream=%d: flushed %d buffered 0-RTT event(s) before ReadLoop",
			streamID, totalBuffered)
	}

	t.streams.ReadLoop(s)
	t.streams.CloseStream(s)
}
