// Package tunnel wires the IMAP transport together with the stream
// manager and the local TCP listener/dialer to form the actual tunnel.
package tunnel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/config"
	titcrypto "github.com/true-imap-tunnel/true-imap-tunnel/internal/crypto"
	imappkg "github.com/true-imap-tunnel/true-imap-tunnel/internal/imap"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/protocol"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/tlog"
)

// Multipath owns every IMAP account: one Sender + one Watcher per account.
//
// By default, outbound frames are routed through a single account per
// stream ("stream affinity"). The account is chosen at stream creation
// time — preferring proven connected accounts — and recorded in the
// route table. All subsequent outbound frames for that stream use the
// same account, so frames travel through a single path with consistent
// latency and the reorder buffer rarely fires.
//
// The experimental frame_round_robin mode relaxes this for DATA frames:
// OPEN/FIN/RST/control frames stay route-affine, while DATA may be
// spread across connected+proven accounts so one active stream can use
// multiple IMAP paths.
//
// Control frames (StreamID == 0) and frames with no recorded route fall
// back to round-robin. If the preferred account is currently
// disconnected, Send walks the remaining senders in order — temporarily
// breaking stickiness but preserving availability. The reorder buffer
// on the receiving end picks up the slack.
//
// All watchers feed a single FrameHandler concurrently. The caller is
// responsible for serialisation if needed (the streams.Manager is
// concurrency-safe).
type Multipath struct {
	cfg      *config.Config
	senders  []*imappkg.Sender
	watchers []*imappkg.Watcher

	rrCounter atomic.Uint64

	// routes maps streamID → preferred account index. Populated by
	// AllocRoute (client side, when opening a new stream) or by
	// PinByLabel (server side, when an OPEN arrives via a given account).
	routes sync.Map

	mu sync.Mutex
	wg sync.WaitGroup

	OnAsyncSendError func(protocol.Frame, error)
}

// ErrNoSenders is returned by Send when no IMAP account is currently
// connected.
var ErrNoSenders = errors.New("no IMAP sender currently connected")

// NewMultipath builds the senders and watchers but does not connect them.
// Call Start to begin running them.
//
// filter, if non-nil, is applied to every watcher: a watcher only
// processes and deletes messages whose frame satisfies the filter.
// Multi-client deployments pass a filter that compares the frame's
// stream client-id to this side's client_id.
//
// aead, if non-nil, encrypts outbound frames (in senders) and decrypts
// inbound frames (in watchers). nil disables encryption.
func NewMultipath(cfg *config.Config, handler imappkg.FrameHandler, filter imappkg.FrameFilter, aead *titcrypto.AEAD) *Multipath {
	m := &Multipath{cfg: cfg}
	for i := range cfg.Accounts {
		acc := &cfg.Accounts[i]
		s := imappkg.NewSender(cfg, acc, aead)
		s.AsyncErrorHandler = func(f protocol.Frame, err error) {
			if m.OnAsyncSendError != nil {
				m.OnAsyncSendError(f, err)
			}
		}
		m.senders = append(m.senders, s)
		w := imappkg.NewWatcher(cfg, acc, handler, aead)
		w.Filter = filter
		m.watchers = append(m.watchers, w)
	}
	return m
}

// Start launches every sender and watcher. It returns immediately; the
// goroutines run until ctx is cancelled.
func (m *Multipath) Start(ctx context.Context) {
	for _, s := range m.senders {
		s := s
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			s.Run(ctx)
		}()
	}
	for _, w := range m.watchers {
		w := w
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			w.Run(ctx)
		}()
	}
}

// Wait blocks until every goroutine started by Start has returned.
func (m *Multipath) Wait() { m.wg.Wait() }

// KickWatchers puts every watcher into active-polling mode. Intended to
// be called right after a successful outbound APPEND so the watchers
// start expecting a response on the short interval.
func (m *Multipath) KickWatchers() {
	for _, w := range m.watchers {
		w.Kick()
	}
}

// provenReconnectGrace is the time window after the last successfully-
// decoded frame during which an account is still considered "proven"
// even if its current framesReceived counter has been reset to 0 by a
// reconnect. Without this grace, the proven tier flaps on every
// backoff cycle: the account drops out of the proven filter, sends
// briefly fan out to other accounts, then the new connection's first
// reply re-promotes it. Smoothing over the reconnect gap keeps routing
// stable for the common transient-failure case.
const provenReconnectGrace = 60 * time.Second

// proven reports whether the account at index idx has shown signs of
// life recently. The primary signal is the paired watcher's
// FramesReceived counter, which proves the path is fully bidirectional
// on the current connection. As a secondary, sticky signal we also
// accept LastFrameAt within provenReconnectGrace \u2014 this avoids a
// routing flap each time the watcher reconnects (FramesReceived resets
// to 0, but LastFrameAt persists).
func (m *Multipath) proven(idx int) bool {
	if idx < 0 || idx >= len(m.watchers) {
		return false
	}
	w := m.watchers[idx]
	if w.FramesReceived() > 0 {
		return true
	}
	if last := w.LastFrameAt(); !last.IsZero() && time.Since(last) < provenReconnectGrace {
		return true
	}
	return false
}

// anyProven reports whether at least one account is proven.
func (m *Multipath) anyProven() bool {
	for i := range m.watchers {
		if m.proven(i) {
			return true
		}
	}
	return false
}

func (m *Multipath) idleSupported(idx int) bool {
	if idx < 0 || idx >= len(m.watchers) {
		return false
	}
	idle, known := m.watchers[idx].IdleSupported()
	return known && idle
}

// idleProvenAvailable reports whether normal traffic should prefer IDLE-capable
// paths. Non-IDLE paths must still remain probeable via SendVia, and remain a
// fallback if every preferred path fails.
func (m *Multipath) idleProvenAvailable() bool {
	if len(m.senders) < 2 {
		return false
	}
	for idx, s := range m.senders {
		if s.Connected() && m.proven(idx) && m.idleSupported(idx) {
			return true
		}
	}
	return false
}

// Send dispatches one frame.
//
// Routing tiers, tried in order:
//
//  1. Proven + connected + IDLE-capable, when such a path is available.
//  2. Proven + connected — accounts whose paired watcher has decoded
//     at least one frame this connection (known bidirectional).
//  3. Connected (any) — bootstrap fallback used only while NO account
//     is proven yet; without it the very first send would have no
//     candidate, and no send means no proof.
//  4. Disconnected — last resort; may have just finished backoff.
//
// A recorded stream-affinity route is honoured first while its sender is
// connected, unless an IDLE-proven path is available and the recorded route is
// non-IDLE. Proof status otherwise only affects fallback/new-route choices.
// Returns ErrNoSenders when no account is configured, or the last error
// when all attempts failed.
func (m *Multipath) Send(f protocol.Frame) error {
	if m.cfg.FrameRoundRobinEnabled() && f.Type == protocol.MsgData && f.StreamID != 0 {
		if err, ok := m.sendFrameRoundRobin(f); ok {
			return err
		}
	}

	n := len(m.senders)
	if n == 0 {
		return ErrNoSenders
	}

	start := -1
	routeIdx := -1
	if f.StreamID != 0 {
		if v, ok := m.routes.Load(f.StreamID); ok {
			routeIdx = v.(int)
			start = routeIdx
		}
	}
	if start < 0 {
		start = int(m.rrCounter.Add(1)-1) % n
	}

	useProvenFilter := m.anyProven()
	preferIdle := m.idleProvenAvailable()
	var lastErr error
	triedRoute := false

	if routeIdx >= 0 && routeIdx < n {
		s := m.senders[routeIdx]
		if s.Connected() && (!preferIdle || m.idleSupported(routeIdx)) {
			triedRoute = true
			if err := m.sendOn(s, f); err != nil {
				tlog.Warnf("multipath: routed sender %s failed: %v", s.Label(), err)
				lastErr = err
			} else {
				m.KickWatchers()
				return nil
			}
		}
	}

	// Tier 1: proven + connected (or all connected when nothing is
	// proven yet).
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if triedRoute && idx == routeIdx {
			continue
		}
		s := m.senders[idx]
		if !s.Connected() {
			continue
		}
		if useProvenFilter && !m.proven(idx) {
			continue
		}
		if preferIdle && !m.idleSupported(idx) {
			continue
		}
		if err := m.sendOn(s, f); err != nil {
			tlog.Warnf("multipath: sender %s failed: %v", s.Label(), err)
			lastErr = err
			continue
		}
		m.KickWatchers()
		return nil
	}

	// If every preferred IDLE-capable path failed, fall back to proven
	// non-IDLE paths rather than dropping traffic.
	if preferIdle {
		for i := 0; i < n; i++ {
			idx := (start + i) % n
			if triedRoute && idx == routeIdx {
				continue
			}
			s := m.senders[idx]
			if !s.Connected() || !m.proven(idx) || m.idleSupported(idx) {
				continue
			}
			if err := m.sendOn(s, f); err != nil {
				tlog.Warnf("multipath: fallback non-IDLE sender %s failed: %v", s.Label(), err)
				lastErr = err
				continue
			}
			m.KickWatchers()
			return nil
		}
	}

	// Tier 2: connected but not proven. Only reachable when some
	// account WAS proven (so useProvenFilter is true) but the
	// proven ones all failed.
	if useProvenFilter {
		for i := 0; i < n; i++ {
			idx := (start + i) % n
			if triedRoute && idx == routeIdx {
				continue
			}
			s := m.senders[idx]
			if !s.Connected() || m.proven(idx) {
				continue
			}
			if err := m.sendOn(s, f); err != nil {
				tlog.Debugf("multipath: unproven sender %s failed: %v",
					s.Label(), err)
				lastErr = err
				continue
			}
			m.KickWatchers()
			return nil
		}
	}

	// Tier 3: disconnected senders may have completed backoff.
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if triedRoute && idx == routeIdx {
			continue
		}
		s := m.senders[idx]
		if s.Connected() {
			continue
		}
		if err := m.sendOn(s, f); err != nil {
			lastErr = err
			continue
		}
		m.KickWatchers()
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("all senders failed: %w", lastErr)
	}
	return ErrNoSenders
}

func (m *Multipath) sendFrameRoundRobin(f protocol.Frame) (error, bool) {
	n := len(m.senders)
	if n == 0 || !m.anyProven() {
		return nil, false
	}
	start := int(m.rrCounter.Add(1)-1) % n
	preferIdle := m.idleProvenAvailable()
	var lastErr error
	tried := false

	for i := 0; i < n; i++ {
		idx := (start + i) % n
		s := m.senders[idx]
		if !s.Connected() || !m.proven(idx) {
			continue
		}
		if preferIdle && !m.idleSupported(idx) {
			continue
		}
		tried = true
		if err := m.sendOn(s, f); err != nil {
			tlog.Warnf("multipath: frame-round-robin sender %s failed: %v", s.Label(), err)
			lastErr = err
			continue
		}
		m.KickWatchers()
		return nil, true
	}
	if tried && lastErr != nil {
		tlog.Debugf("multipath: frame-round-robin falling back to stream route after failure: %v", lastErr)
	}
	return nil, false
}

func (m *Multipath) sendOn(s *imappkg.Sender, f protocol.Frame) error {
	if m.cfg.AsyncDataSendEnabled() && f.Type == protocol.MsgData {
		return s.Enqueue(f)
	}
	return s.Send(f)
}

// AllocRoute picks an account for a new stream and records it.
//
// Preference order:
//  1. Proven + connected + IDLE-capable, when such a path is available.
//  2. Proven + connected (paired watcher has decoded ≥ 1 frame).
//  3. First connected sender (bootstrap path used until any account is
//     proven). Real streams intentionally do not round-robin across
//     unproven paths; the ping loop probes each path independently.
//  4. Round-robin slot regardless of state, as a last-resort default.
//
// Returns the chosen index, or -1 if no accounts are configured.
// Called by the client side when allocating a new stream ID.
func (m *Multipath) AllocRoute(streamID uint32) int {
	n := len(m.senders)
	if n == 0 {
		return -1
	}
	m.AllowStream(streamID)
	start := int(m.rrCounter.Add(1)-1) % n
	useProvenFilter := m.anyProven()
	preferIdle := m.idleProvenAvailable()

	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if m.senders[idx].Connected() &&
			(!useProvenFilter || m.proven(idx)) &&
			(!preferIdle || m.idleSupported(idx)) {
			m.routes.Store(streamID, idx)
			return idx
		}
	}
	if preferIdle {
		for i := 0; i < n; i++ {
			idx := (start + i) % n
			if m.senders[idx].Connected() && (!useProvenFilter || m.proven(idx)) {
				m.routes.Store(streamID, idx)
				return idx
			}
		}
	}
	// Bootstrap fallback: before any path is proven, keep application
	// streams on the first connected sender. The ping loop sends
	// control probes through every account; only paths that answer
	// should enter normal stream rotation.
	if !useProvenFilter {
		for idx, s := range m.senders {
			if s.Connected() {
				m.routes.Store(streamID, idx)
				return idx
			}
		}
	} else {
		for i := 0; i < n; i++ {
			idx := (start + i) % n
			if m.senders[idx].Connected() {
				m.routes.Store(streamID, idx)
				return idx
			}
		}
	}
	m.routes.Store(streamID, start)
	return start
}

// PinByLabel records a route for streamID using the account with the
// matching label. Called by the server side when an OPEN frame arrives,
// to pin outbound traffic for the new stream to the same path the OPEN
// arrived on. Returns true if a matching account was found.
func (m *Multipath) PinByLabel(streamID uint32, label string) bool {
	m.AllowStream(streamID)
	for i, s := range m.senders {
		if s.Label() == label {
			m.routes.Store(streamID, i)
			return true
		}
	}
	return false
}

// RemoveRoute clears the route table entry for streamID. Intended to be
// wired to streams.Manager.OnRemove so stale routes don't accumulate.
func (m *Multipath) RemoveRoute(streamID uint32) {
	m.routes.Delete(streamID)
}

// CancelStream tells every sender to drop queued DATA/FIN for streamID. This is
// used when a stream is locally closed (especially after receiving a peer RST)
// so async DATA already sitting in sender queues does not keep leaking into IMAP.
// RST frames are never dropped.
func (m *Multipath) CancelStream(streamID uint32) {
	for _, s := range m.senders {
		s.CancelStream(streamID)
	}
}

// AllowStream clears a previous cancellation tombstone when a stream ID is
// deliberately reintroduced by a fresh OPEN.
func (m *Multipath) AllowStream(streamID uint32) {
	for _, s := range m.senders {
		s.AllowStream(streamID)
	}
}

// RouteLabel returns the label of the account pinned for streamID, or
// "" if no route is recorded. Useful in logs.
func (m *Multipath) RouteLabel(streamID uint32) string {
	v, ok := m.routes.Load(streamID)
	if !ok {
		return ""
	}
	idx := v.(int)
	if idx < 0 || idx >= len(m.senders) {
		return ""
	}
	return m.senders[idx].Label()
}

// AnyConnected reports whether at least one configured IMAP account
// currently holds an open session.
func (m *Multipath) AnyConnected() bool {
	for _, s := range m.senders {
		if s.Connected() {
			return true
		}
	}
	return false
}

// AnyReceiveReady reports whether at least one watcher has selected its
// receive mailbox and established the UID baseline used to process future
// messages.
func (m *Multipath) AnyReceiveReady() bool {
	for _, w := range m.watchers {
		if w.ReceiveReady() {
			return true
		}
	}
	return false
}

// AllConnected reports whether every configured IMAP account currently
// holds an open session.
func (m *Multipath) AllConnected() bool {
	for _, s := range m.senders {
		if !s.Connected() {
			return false
		}
	}
	return len(m.senders) > 0
}

// ConnectedCount returns the number of currently connected senders.
func (m *Multipath) ConnectedCount() int {
	n := 0
	for _, s := range m.senders {
		if s.Connected() {
			n++
		}
	}
	return n
}

// ProvenCount returns the number of accounts whose watcher has decoded at
// least one frame since the last reconnect.
func (m *Multipath) ProvenCount() int {
	n := 0
	for i := range m.watchers {
		if m.proven(i) {
			n++
		}
	}
	return n
}

// PathProven reports whether the account at idx has delivered a frame recently.
// Out-of-range indexes are treated as not proven.
func (m *Multipath) PathProven(idx int) bool {
	return m.proven(idx)
}

// SenderCount returns the total number of senders.
func (m *Multipath) SenderCount() int { return len(m.senders) }

// SenderConnected reports whether the sender at idx currently holds an
// open connection. Out-of-range indexes are treated as disconnected.
func (m *Multipath) SenderConnected(idx int) bool {
	if idx < 0 || idx >= len(m.senders) {
		return false
	}
	return m.senders[idx].Connected()
}

// ReceiveReady reports whether the watcher at idx has selected its mailbox and
// established the UID baseline used to process future messages.
func (m *Multipath) ReceiveReady(idx int) bool {
	if idx < 0 || idx >= len(m.watchers) {
		return false
	}
	return m.watchers[idx].ReceiveReady()
}

// SendVia dispatches one frame through a specific sender by index.
// Used by the ping loop to probe each account independently, ensuring
// every path gets proven. Returns ErrNoSenders if idx is out of range.
func (m *Multipath) SendVia(idx int, f protocol.Frame) error {
	if idx < 0 || idx >= len(m.senders) {
		return ErrNoSenders
	}
	err := m.sendOn(m.senders[idx], f)
	if err == nil {
		m.KickWatchers()
	}
	return err
}

// SendViaLabel dispatches one frame through the sender whose label
// matches. Falls back to the normal Send router if no match is found.
// Used to echo Pong back through the same account the Ping arrived on,
// so each path independently becomes proven.
func (m *Multipath) SendViaLabel(label string, f protocol.Frame) error {
	for i, s := range m.senders {
		if s.Label() == label {
			return m.SendVia(i, f)
		}
	}
	return m.Send(f)
}

// SenderSentCounts returns the per-account frame counts in the same
// order as the configured accounts. Intended for tests and diagnostics.
// One frame batched with N siblings into a single APPEND still counts
// as one — use SenderBatchCounts for the APPEND-command metric.
func (m *Multipath) SenderSentCounts() []uint64 {
	out := make([]uint64, len(m.senders))
	for i, s := range m.senders {
		out[i] = s.SentCount()
	}
	return out
}

// SenderBatchCounts returns the per-account APPEND-command counts.
// The ratio SenderSentCounts[i] / SenderBatchCounts[i] is the average
// batch size for that account — useful for confirming that cross-
// stream batching kicks in under contention.
func (m *Multipath) SenderBatchCounts() []uint64 {
	out := make([]uint64, len(m.senders))
	for i, s := range m.senders {
		out[i] = s.BatchCount()
	}
	return out
}

// SenderLabels returns the per-account labels, parallel to
// SenderSentCounts.
func (m *Multipath) SenderLabels() []string {
	out := make([]string, len(m.senders))
	for i, s := range m.senders {
		out[i] = s.Label()
	}
	return out
}
