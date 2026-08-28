// Package streams multiplexes many TCP connections over a single
// frame-oriented transport. It owns stream lifecycle and frame
// reordering — concerns that are independent of the underlying
// transport (here, IMAP messages).
//
// The design is adapted from github.com/bridge-to-freedom/adapter, which
// solves the same multiplexing problem over WebSocket. Key features:
//
//   - Cross-stream write batching: individual TCP reads are sent as
//     frames immediately (no per-stream buffering). The downstream
//     IMAP sender opportunistically packs whatever frames are queued
//     into a single APPEND, so concurrent streams naturally share
//     round-trips without any artificial delay. While a caller's
//     SendFrame blocks on the in-flight APPEND, the kernel's TCP
//     receive buffer accumulates bytes — the next Read returns a
//     bigger chunk, providing natural backpressure-driven coalescing.
//
//   - Reorder buffer: incoming DATA/FIN/RST frames are buffered per-stream
//     and delivered in SeqID order. Essential when multiple IMAP accounts
//     are used as parallel transport paths: independent paths have
//     independent latency, so frames can arrive out of order.
package streams

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/noiseonwires/true-imap-tunnel/internal/protocol"
	"github.com/noiseonwires/true-imap-tunnel/internal/tlog"
)

// SendFunc sends an encoded frame to the peer.
type SendFunc func(f protocol.Frame) error

// Stream represents one multiplexed TCP connection.
//
// Each Stream owns a small bounded outbound queue and a dedicated
// writer goroutine. Frames flow:
//
//	watcher → Manager.HandleData → s.outbound channel → writer → TCP
//
// This decoupling is critical: without it, a slow TCP consumer would
// block the watcher's dispatch loop, head-of-line-blocking every other
// stream sharing the same IMAP mailbox.
//
// If the queue overflows (writer can't keep up), the offending stream
// uses a bounded per-stream overflow queue with a timeout before RST:
// the watcher keeps moving, transient TCP backpressure gets a chance to
// clear, and other streams stay healthy. The peer learns of the RST via
// a frame sent over the transport.
type Stream struct {
	ID   uint32
	Conn net.Conn

	mu     sync.Mutex
	closed bool

	// outbound is a small bounded channel of pending writes for this
	// stream. Populated by HandleData/HandleFin, drained by the
	// dedicated writer goroutine started in Register.
	outbound chan outboundEvent

	// overflow is a bounded FIFO used only when outbound is full. It is
	// drained by at most one goroutine per stream, preserving DATA/FIN
	// order without blocking the watcher dispatch goroutine.
	overflowMu       sync.Mutex
	overflow         []outboundEvent
	overflowDraining bool

	// closeCh is closed by CloseStream to signal the writer goroutine
	// to exit (in addition to net.Conn.Close interrupting any blocked
	// Write). It is closed exactly once, under mu.
	closeCh chan struct{}

	// writerDone is closed by the writer goroutine when it exits, so
	// CloseStream can optionally wait for it.
	writerDone chan struct{}

	// rstScheduled is set the first time a watcher dispatch encounters
	// the outbound queue full and decides to RST the stream. Without
	// this guard, a burst of in-flight frames after the stuck-write
	// condition would each spawn their own RST goroutine, flooding the
	// sender's queue and head-of-line-blocking healthy streams.
	rstScheduled atomic.Bool
}

// outboundEvent is one pending action for the per-stream writer.
type outboundEvent struct {
	kind    byte // protocol.MsgData or protocol.MsgFin
	payload []byte
}

// Manager tracks active streams and dispatches frames between TCP and the
// peer transport.
type Manager struct {
	mu      sync.Mutex
	streams map[uint32]*Stream
	send    SendFunc

	// nextID allocates outbound stream IDs. Only the client side allocates
	// new IDs; the server echoes the ID from each incoming OPEN.
	nextID atomic.Uint32

	// Reorder controls whether incoming stream frames are buffered and
	// delivered in SeqID order. Should be true on both sides whenever
	// multipath is in use.
	Reorder bool

	// MaxReorderPending bounds how many out-of-order frames can wait for
	// one missing SeqID before the stream is reset locally. Default 1024.
	MaxReorderPending int

	// MaxReorderDelay bounds how long an out-of-order gap can remain open
	// before the stream is reset locally. Default 30s.
	MaxReorderDelay time.Duration

	// InboundQueueSize is the per-stream outbound (TCP-write) buffer
	// depth. The watcher dispatch goroutine enqueues frames here; a
	// dedicated per-stream writer drains them. Bounding this prevents
	// memory blow-up when a TCP consumer is slow. If the queue fills, a
	// per-stream overflow drainer waits up to OutboundQueueWait before
	// resetting the stream, so normal TCP backpressure slows only that
	// stream instead of blocking the shared watcher dispatch loop.
	InboundQueueSize int

	// OutboundQueueWait bounds how long overflowed incoming DATA/FIN may
	// wait for per-stream TCP-write queue space before the stream is
	// considered stuck and reset. Default 30s.
	OutboundQueueWait time.Duration

	// OnRemove, if set, is invoked from Remove (and therefore from
	// CloseStream / CloseAll) after the stream has been unregistered.
	// Used by upper layers to clean up per-stream bookkeeping (e.g. the
	// multipath route table).
	OnRemove func(streamID uint32)

	// OnReorderReset, if set, is invoked when the reorder buffer gives
	// up on a missing SeqID and resets the stream locally. Upper layers
	// use this to notify the peer with a real RST so it stops sending.
	OnReorderReset func(streamID uint32)

	// Per-stream send sequence counters (auto-incremented in SendFrame).
	seqCounters sync.Map // streamID → *atomic.Uint32

	reorderMu   sync.Mutex
	reorderBufs map[uint32]*reorderBuf
	// closedReorder tombstones recently-closed stream IDs so late DATA/FIN
	// frames from already-aborted streams are dropped instead of creating
	// fresh reorder buffers that wait forever for SeqID 1.
	closedReorder map[uint32]time.Time
	// lastReorderSweep is the time of the last orphan-buffer GC sweep.
	// Guarded by reorderMu. The sweep runs lazily from DispatchFrame at
	// most once per MaxReorderDelay, deleting buffers whose stream is
	// not registered and whose lastSeen is older than MaxReorderDelay.
	lastReorderSweep time.Time
}

// reorderBuf holds out-of-order frames for a single stream.
type reorderBuf struct {
	mu       sync.Mutex
	expected uint32
	pending  map[uint32]protocol.Frame
	gapSince time.Time
	// lastSeen records the wall time of the most recent frame observed
	// for this buffer. Used by the orphan-buffer GC sweep so we can drop
	// reorderBuf entries whose stream is unknown / never registered and
	// have been idle for longer than MaxReorderDelay — preventing a peer
	// that ships frames for nonexistent stream-IDs from accumulating
	// unbounded entries in m.reorderBufs.
	lastSeen time.Time
}

// NewManager constructs a Manager. send is invoked from arbitrary goroutines
// and MUST be safe for concurrent use.
func NewManager(send SendFunc) *Manager {
	m := &Manager{
		streams:           make(map[uint32]*Stream),
		send:              send,
		reorderBufs:       make(map[uint32]*reorderBuf),
		closedReorder:     make(map[uint32]time.Time),
		InboundQueueSize:  64,
		OutboundQueueWait: 30 * time.Second,
		MaxReorderPending: 1024,
		MaxReorderDelay:   30 * time.Second,
	}
	m.nextID.Store(randomInitialStreamID())
	return m
}

// NextID allocates a fresh outbound stream ID. Client side only.
func (m *Manager) NextID() uint32 {
	for {
		id := (m.nextID.Add(1) - 1) & protocol.StreamLocalIDMask
		if id != 0 {
			return id
		}
	}
}

func randomInitialStreamID() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err == nil {
		if id := binary.BigEndian.Uint32(b[:]) & protocol.StreamLocalIDMask; id != 0 {
			return id
		}
	}
	if id := uint32(time.Now().UnixNano()) & protocol.StreamLocalIDMask; id != 0 {
		return id
	}
	return 1
}

// Register adds a stream to the manager and starts its per-stream
// writer goroutine. Safe to call after the peer has confirmed the
// stream (OPEN_OK) or for a server-side stream that has just been
// dialled.
//
// The stream's outbound queue and lifecycle channels are initialised
// here (callers should not pre-populate them on the Stream value).
func (m *Manager) Register(s *Stream) {
	qSize := m.InboundQueueSize
	if qSize <= 0 {
		qSize = 64
	}
	s.outbound = make(chan outboundEvent, qSize)
	s.closeCh = make(chan struct{})
	s.writerDone = make(chan struct{})

	m.mu.Lock()
	m.streams[s.ID] = s
	m.mu.Unlock()

	go m.runWriter(s)
}

// runWriter drains s.outbound, performing each pending TCP operation
// in arrival order. Exits on Conn-write error, FIN, or closeCh signal.
func (m *Manager) runWriter(s *Stream) {
	defer close(s.writerDone)
	for {
		select {
		case <-s.closeCh:
			return
		case ev := <-s.outbound:
			switch ev.kind {
			case protocol.MsgData:
				if _, err := s.Conn.Write(ev.payload); err != nil {
					tlog.Warnf("write to TCP failed stream=%d err=%v",
						s.ID, err)
					m.CloseStream(s)
					return
				}
			case protocol.MsgFin:
				// Half-close write side; remaining queued DATA already
				// drained (FIN is enqueued after any DATA from the peer).
				if tc, ok := s.Conn.(*net.TCPConn); ok {
					_ = tc.CloseWrite()
				}
				return
			}
		}
	}
}

// Get returns a stream by ID, or nil.
func (m *Manager) Get(id uint32) *Stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[id]
}

// Remove unregisters a stream and discards its sequencing state.
func (m *Manager) Remove(id uint32) {
	if m.Reorder {
		m.reorderMu.Lock()
		delete(m.reorderBufs, id)
		if m.closedReorder == nil {
			m.closedReorder = make(map[uint32]time.Time)
		}
		m.closedReorder[id] = time.Now().Add(m.reorderTombstoneDuration())
		m.reorderMu.Unlock()
	}
	m.mu.Lock()
	delete(m.streams, id)
	m.mu.Unlock()
	m.seqCounters.Delete(id)
	if m.OnRemove != nil {
		m.OnRemove(id)
	}
}

// SendFrame stamps a SeqID (for stream frames) and dispatches the frame.
func (m *Manager) SendFrame(f protocol.Frame) error {
	if f.StreamID > 0 && protocol.IsOrdered(f.Type) {
		v, _ := m.seqCounters.LoadOrStore(f.StreamID, &atomic.Uint32{})
		f.SeqID = v.(*atomic.Uint32).Add(1)
	}
	return m.send(f)
}

// HandleData enqueues payload for write to the stream's TCP connection.
// This is invoked from the IMAP watcher dispatch goroutine; doing the
// TCP Write synchronously here would head-of-line-block every other
// stream sharing the same mailbox if this stream's consumer is slow.
// Instead, the payload is queued and a per-stream writer goroutine
// drains it (see Register / runWriter).
//
// If the queue is full, this function hands the event to a per-stream
// overflow drainer and returns immediately. The drainer waits briefly
// for the writer to catch up, applying TCP-like backpressure to this
// stream without blocking the shared watcher dispatch loop. If the queue
// stays full past OutboundQueueWait, the stream is reset.
func (m *Manager) HandleData(streamID uint32, payload []byte) {
	s := m.Get(streamID)
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	out := s.outbound
	closeCh := s.closeCh
	s.mu.Unlock()

	ev := outboundEvent{kind: protocol.MsgData, payload: payload}

	m.enqueueOutboundOrReset(s, streamID, out, closeCh, ev)
}

// rstSlowStream tears down a stream whose TCP consumer cannot keep up,
// and notifies the peer so it stops sending data we cannot drain.
func (m *Manager) rstSlowStream(s *Stream, streamID uint32) {
	m.CloseStream(s)
	if m.send != nil {
		_ = m.send(protocol.Frame{Type: protocol.MsgRst, StreamID: streamID})
	}
}

// HandleFin handles a graceful half-close from the peer. Enqueued so it
// is delivered AFTER any pending DATA frames already in the writer
// queue — preserving the peer's intended ordering.
//
// If the queue is full, FIN follows the same per-stream overflow policy
// as DATA so it cannot overtake queued DATA or be dropped during a
// temporary upload burst.
func (m *Manager) HandleFin(streamID uint32) {
	s := m.Get(streamID)
	if s == nil {
		return
	}
	tlog.Debugf("FIN handling stream=%d", streamID)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	out := s.outbound
	closeCh := s.closeCh
	s.mu.Unlock()

	m.enqueueOutboundOrReset(s, streamID, out, closeCh, outboundEvent{kind: protocol.MsgFin})
}

func (m *Manager) enqueueOutboundOrReset(s *Stream, streamID uint32, out chan outboundEvent, closeCh chan struct{}, ev outboundEvent) {
	if s.rstScheduled.Load() {
		return
	}

	s.overflowMu.Lock()
	if s.overflowDraining || len(s.overflow) > 0 {
		m.enqueueOverflowLocked(s, streamID, out, closeCh, ev)
		s.overflowMu.Unlock()
		return
	}

	select {
	case out <- ev:
		s.overflowMu.Unlock()
		return
	case <-closeCh:
		s.overflowMu.Unlock()
		return
	default:
	}

	m.enqueueOverflowLocked(s, streamID, out, closeCh, ev)
	s.overflowMu.Unlock()
}

func (m *Manager) enqueueOverflowLocked(s *Stream, streamID uint32, out chan outboundEvent, closeCh chan struct{}, ev outboundEvent) {
	if len(s.overflow) >= m.overflowLimit() {
		m.scheduleSlowStreamReset(s, streamID, "outbound overflow queue full")
		return
	}
	s.overflow = append(s.overflow, ev)
	if s.overflowDraining {
		return
	}
	s.overflowDraining = true
	go m.drainOverflow(s, streamID, out, closeCh)
}

func (m *Manager) overflowLimit() int {
	limit := m.InboundQueueSize
	if limit <= 0 {
		limit = 64
	}
	return limit
}

func (m *Manager) drainOverflow(s *Stream, streamID uint32, out chan outboundEvent, closeCh chan struct{}) {
	for {
		s.overflowMu.Lock()
		if len(s.overflow) == 0 {
			s.overflowDraining = false
			s.overflowMu.Unlock()
			return
		}
		ev := s.overflow[0]
		s.overflowMu.Unlock()

		if !m.sendOutboundWithWait(s, streamID, out, closeCh, ev) {
			return
		}

		s.overflowMu.Lock()
		if len(s.overflow) > 0 {
			copy(s.overflow, s.overflow[1:])
			s.overflow[len(s.overflow)-1] = outboundEvent{}
			s.overflow = s.overflow[:len(s.overflow)-1]
		}
		s.overflowMu.Unlock()
	}
}

func (m *Manager) sendOutboundWithWait(s *Stream, streamID uint32, out chan outboundEvent, closeCh chan struct{}, ev outboundEvent) bool {
	select {
	case out <- ev:
		return true
	case <-closeCh:
		return false
	default:
	}

	wait := m.OutboundQueueWait
	if wait <= 0 {
		m.scheduleSlowStreamReset(s, streamID, "outbound queue full")
		return false
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case out <- ev:
		return true
	case <-closeCh:
		return false
	case <-timer.C:
		m.scheduleSlowStreamReset(s, streamID, "outbound queue full too long")
		return false
	}
}

func (m *Manager) scheduleSlowStreamReset(s *Stream, streamID uint32, reason string) {
	if !s.rstScheduled.CompareAndSwap(false, true) {
		return
	}
	tlog.Warnf("stream=%d %s; RSTing slow consumer", streamID, reason)
	go m.rstSlowStream(s, streamID)
}

// HandleRst aborts a stream immediately.
func (m *Manager) HandleRst(streamID uint32) {
	s := m.Get(streamID)
	if s == nil {
		return
	}
	tlog.Debugf("RST handling stream=%d", streamID)
	m.CloseStream(s)
}

// CloseStream closes the TCP connection and removes the stream from
// tracking. Safe to call repeatedly.
func (m *Manager) CloseStream(s *Stream) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	closeCh := s.closeCh
	s.mu.Unlock()
	tlog.Debugf("closing stream=%d", s.ID)
	if closeCh != nil {
		close(closeCh)
	}
	_ = s.Conn.Close()
	m.Remove(s.ID)
}

// CloseAll closes every active stream.
func (m *Manager) CloseAll() {
	m.mu.Lock()
	all := make([]*Stream, 0, len(m.streams))
	for _, s := range m.streams {
		all = append(all, s)
	}
	m.mu.Unlock()

	if len(all) > 0 {
		tlog.Infof("closing all %d streams", len(all))
	}
	for _, s := range all {
		m.CloseStream(s)
	}

	m.seqCounters.Range(func(k, _ any) bool {
		m.seqCounters.Delete(k)
		return true
	})
	if m.Reorder {
		m.reorderMu.Lock()
		m.reorderBufs = make(map[uint32]*reorderBuf)
		m.reorderMu.Unlock()
	}
}

// Count returns the number of active streams.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.streams)
}

func (m *Manager) reorderDelay() time.Duration {
	if m.MaxReorderDelay <= 0 {
		return 30 * time.Second
	}
	return m.MaxReorderDelay
}

func (m *Manager) reorderTombstoneDuration() time.Duration {
	d := 4 * m.reorderDelay()
	if d < 30*time.Second {
		return 30 * time.Second
	}
	return d
}

func (m *Manager) tombstoneReorderStream(streamID uint32, now time.Time) {
	m.reorderMu.Lock()
	delete(m.reorderBufs, streamID)
	if m.closedReorder == nil {
		m.closedReorder = make(map[uint32]time.Time)
	}
	m.closedReorder[streamID] = now.Add(m.reorderTombstoneDuration())
	m.reorderMu.Unlock()
}

// sweepOrphanReorderBufsLocked drops reorderBuf entries whose stream
// is not registered and whose lastSeen is older than idleBudget.
//
// MUST be called with m.reorderMu held. Briefly acquires m.mu to test
// stream membership; the lock order (reorderMu \u2192 mu) is unique to this
// path (other callers that touch both locks release m.mu before
// acquiring reorderMu), so there is no deadlock risk.
//
// The sweep is bounded by len(m.reorderBufs) and runs at most once per
// MaxReorderDelay, so the cost is amortised across many DispatchFrame
// calls.
func (m *Manager) sweepOrphanReorderBufsLocked(now time.Time, idleBudget time.Duration) {
	for id, until := range m.closedReorder {
		if now.After(until) {
			delete(m.closedReorder, id)
		}
	}
	if len(m.reorderBufs) == 0 {
		return
	}
	var stale []uint32
	for id, rb := range m.reorderBufs {
		rb.mu.Lock()
		lastSeen := rb.lastSeen
		rb.mu.Unlock()
		if !lastSeen.IsZero() && now.Sub(lastSeen) <= idleBudget {
			continue
		}
		stale = append(stale, id)
	}
	if len(stale) == 0 {
		return
	}
	m.mu.Lock()
	dropped := 0
	for _, id := range stale {
		if _, registered := m.streams[id]; registered {
			continue
		}
		delete(m.reorderBufs, id)
		dropped++
	}
	m.mu.Unlock()
	if dropped > 0 {
		tlog.Debugf("reorder: swept %d orphan buffer(s)", dropped)
	}
}

// ActiveIDs returns a snapshot of every currently-registered stream
// ID. Useful for shutdown paths that need to enumerate streams while
// new ones may still be added.
func (m *Manager) ActiveIDs() []uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]uint32, 0, len(m.streams))
	for id := range m.streams {
		out = append(out, id)
	}
	return out
}

// DispatchFrame delivers an incoming frame to handler, optionally
// reordering by SeqID. handler is called from the same goroutine as
// DispatchFrame (possibly multiple times in a row when buffered frames
// become deliverable), and calls are serialized per stream. handler must
// not block on send-side operations for this stream, to avoid
// head-of-line blocking the watcher.
func (m *Manager) DispatchFrame(f protocol.Frame, handler func(protocol.Frame)) {
	if !m.Reorder || f.SeqID == 0 || f.StreamID == 0 || !protocol.IsOrdered(f.Type) {
		handler(f)
		return
	}

	now := time.Now()
	maxDelay := m.reorderDelay()
	m.reorderMu.Lock()
	if f.Type == protocol.MsgOpen && f.SeqID == 1 {
		delete(m.closedReorder, f.StreamID)
	} else if until, ok := m.closedReorder[f.StreamID]; ok {
		if now.Before(until) {
			m.reorderMu.Unlock()
			return
		}
		delete(m.closedReorder, f.StreamID)
	}
	rb, ok := m.reorderBufs[f.StreamID]
	if !ok {
		rb = &reorderBuf{expected: 1, pending: make(map[uint32]protocol.Frame), lastSeen: now}
		m.reorderBufs[f.StreamID] = rb
	}
	// Lazy GC: at most once per MaxReorderDelay, walk the buffer table
	// and delete entries that have no registered stream and have been
	// idle past the reorder-delay budget. Without this, frames for
	// stream-IDs that never receive an OPEN (and thus never trigger
	// CloseStream→Remove) would leak reorderBuf entries indefinitely.
	if now.Sub(m.lastReorderSweep) > maxDelay {
		m.lastReorderSweep = now
		m.sweepOrphanReorderBufsLocked(now, maxDelay)
	}
	m.reorderMu.Unlock()

	var deliver []protocol.Frame
	var reset bool

	rb.mu.Lock()

	rb.lastSeen = now

	// An OPEN frame (always seq=1) signals a brand-new stream
	// lifecycle. If the reorder buffer already has a higher expected
	// value, the previous stream with this ID ended without a clean
	// teardown (client crash, network drop, etc.) and the old
	// expected counter would silently discard every frame from the
	// new session as "duplicate/old". Reset it.
	if f.Type == protocol.MsgOpen && f.SeqID == 1 && rb.expected > 1 {
		tlog.Debugf("reorder: resetting stale buffer stream=%d (was expected=%d)",
			f.StreamID, rb.expected)
		rb.expected = 1
		rb.gapSince = time.Time{}
		for k := range rb.pending {
			delete(rb.pending, k)
		}
	}

	if f.SeqID == rb.expected {
		deliver = append(deliver, f)
		rb.expected++
		for {
			next, ok := rb.pending[rb.expected]
			if !ok {
				break
			}
			delete(rb.pending, rb.expected)
			deliver = append(deliver, next)
			rb.expected++
		}
		if len(rb.pending) == 0 {
			rb.gapSince = time.Time{}
		}
	} else if f.SeqID > rb.expected {
		// Copy the payload — the caller's buffer is short-lived.
		cp := make([]byte, len(f.Payload))
		copy(cp, f.Payload)
		f.Payload = cp
		rb.pending[f.SeqID] = f
		if rb.gapSince.IsZero() {
			rb.gapSince = time.Now()
		}
		if len(rb.pending)%64 == 0 {
			tlog.Warnf("reorder buffer growing stream=%d pending=%d expected=%d got=%d",
				f.StreamID, len(rb.pending), rb.expected, f.SeqID)
		}
		maxPending := m.MaxReorderPending
		if maxPending <= 0 {
			maxPending = 1024
		}
		maxDelay := m.MaxReorderDelay
		if maxDelay <= 0 {
			maxDelay = 30 * time.Second
		}
		if len(rb.pending) > maxPending || time.Since(rb.gapSince) > maxDelay {
			tlog.Warnf("reorder gap exceeded stream=%d pending=%d expected=%d got=%d age=%v; resetting stream",
				f.StreamID, len(rb.pending), rb.expected, f.SeqID, time.Since(rb.gapSince).Round(time.Millisecond))
			rb.pending = make(map[uint32]protocol.Frame)
			rb.gapSince = time.Time{}
			// Reset the expected counter so a subsequent OPEN (seq=1) or
			// follow-on frames don't all get logged as "duplicate/old".
			// The RST we hand off below tears the stream down on this side;
			// the peer, on receiving its own RST or on its next OPEN,
			// starts a fresh seq=1 cycle that lines up with expected=1.
			rb.expected = 1
			reset = true
		}
	} else {
		tlog.Warnf("duplicate/old frame stream=%d seq=%d expected=%d",
			f.StreamID, f.SeqID, rb.expected)
	}
	if reset {
		rb.mu.Unlock()
		m.tombstoneReorderStream(f.StreamID, now)
		if m.OnReorderReset != nil {
			m.OnReorderReset(f.StreamID)
		}
		handler(protocol.Frame{Type: protocol.MsgRst, StreamID: f.StreamID})
		return
	}
	for _, df := range deliver {
		handler(df)
	}
	rb.mu.Unlock()
}

// ReadLoop reads from the stream's TCP connection and sends DATA frames to
// the peer. On clean EOF it sends FIN; on error it sends RST. Returns when
// the stream is finished from this side.
//
// Each TCP read is sent as a separate DATA frame immediately. The
// downstream IMAP sender handles cross-stream batching: while the
// blocking SendFrame call waits for the in-flight APPEND to complete,
// the kernel's TCP receive buffer accumulates more bytes — so the next
// Read naturally returns a larger chunk, providing backpressure-driven
// coalescing without any artificial delay.
func (m *Manager) ReadLoop(s *Stream) {
	buf := make([]byte, 32*1024)

	var sentTerm bool
	sendTerm := func(t byte) {
		if sentTerm {
			return
		}
		sentTerm = true
		if err := m.SendFrame(protocol.Frame{Type: t, StreamID: s.ID}); err != nil {
			tlog.Warnf("send %s failed stream=%d err=%v",
				protocol.TypeName(t), s.ID, err)
		}
	}

	sendFin := true
	defer func() {
		s.mu.Lock()
		wasClosed := s.closed
		s.mu.Unlock()
		if sendFin && !wasClosed {
			sendTerm(protocol.MsgFin)
		}
	}()

	for {
		n, err := s.Conn.Read(buf)
		if n > 0 {
			payload := make([]byte, n)
			copy(payload, buf[:n])
			if err2 := m.SendFrame(protocol.Frame{
				Type:     protocol.MsgData,
				StreamID: s.ID,
				Payload:  payload,
			}); err2 != nil {
				tlog.Warnf("send DATA failed stream=%d err=%v", s.ID, err2)
				sendFin = false
				sendTerm(protocol.MsgRst)
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				sendFin = false
				s.mu.Lock()
				closed := s.closed
				s.mu.Unlock()
				if !closed {
					tlog.Warnf("read from TCP failed stream=%d err=%v", s.ID, err)
					sendTerm(protocol.MsgRst)
				}
			}
			return
		}
	}
}
