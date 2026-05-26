package imap

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	imap "github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/config"
	titcrypto "github.com/true-imap-tunnel/true-imap-tunnel/internal/crypto"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/protocol"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/tlog"
)

// Sender owns one IMAP connection used exclusively for APPENDing
// frames to one configured account.
//
// Frames are not APPENDed directly by the caller. They are enqueued
// onto a buffered channel and a dedicated goroutine (started by Run)
// drains the queue, opportunistically packing whatever is available
// into a single batch APPEND. This means:
//
//   - Single-stream workloads: queue contains 1 frame when the loop
//     wakes; after the optional batch delay it is APPENDed (possibly
//     with other frames that arrived during the delay).
//   - Many concurrent streams: while one APPEND is in flight, more
//     frames pile up in the queue. When the APPEND completes the
//     loop drains the queue (up to BatchMaxFrames / BatchMaxBytes)
//     and APPENDs everything in one go. One IMAP round-trip carries
//     N frames. The batch delay is not applied in this case — frames
//     are already waiting.
//
// Send blocks until its frame's batch has been APPENDed (or failed),
// so per-call error reporting is preserved.
//
// On any send error the underlying connection is closed and a
// reconnect is attempted in the loop's next cycle.
type Sender struct {
	acc  *config.AccountConfig
	cfg  *config.Config
	keys *titcrypto.KeyRing

	queue chan *sendReq
	// pending holds requests drained from queue that cannot share an IMAP
	// message with the current batch because they target a different client.
	pending []*sendReq

	mu        sync.Mutex
	client    *imapclient.Client
	lastFail  time.Time
	failDelay time.Duration

	sentCount  atomic.Uint64
	batchCount atomic.Uint64

	connected     atomic.Bool
	connects      atomic.Uint64
	connectedAtNS atomic.Int64

	canceled sync.Map // streamID -> time.Time deadline for dropping stale DATA/FIN

	// AsyncErrorHandler is invoked for async DATA requests when their
	// eventual APPEND fails. It lets upper layers reset the affected
	// stream instead of silently dropping bytes and leaving the peer's
	// reorder buffer waiting for a missing SeqID.
	AsyncErrorHandler func(protocol.Frame, error)

	appendHook func([]byte) error
}

const (
	appendMaxAttempts = 3
	appendRetryDelay  = 250 * time.Millisecond

	streamCancelTTL = 2 * time.Minute
)

var errStreamCanceled = errors.New("stream canceled")

// sendReq is a queued APPEND request from a caller of Send.
type sendReq struct {
	frame protocol.Frame
	reply chan error
}

// NewSender constructs (but does not connect) a Sender. keys may be
// nil to disable per-frame encryption.
func NewSender(cfg *config.Config, acc *config.AccountConfig, keys *titcrypto.KeyRing) *Sender {
	return &Sender{
		acc:       acc,
		cfg:       cfg,
		keys:      keys,
		queue:     make(chan *sendReq, cfg.BatchQueueSize()),
		failDelay: cfg.ReconnectInitialDelay(),
	}
}

// Label returns the account's label for logs.
func (s *Sender) Label() string { return s.acc.Label() }

// Connected reports whether the sender currently holds an open connection.
func (s *Sender) Connected() bool { return s.connected.Load() }

// ConnectedAt returns the time of the most recent successful connection.
func (s *Sender) ConnectedAt() time.Time {
	ns := s.connectedAtNS.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// ConnectCount returns the number of successful IMAP connections.
func (s *Sender) ConnectCount() uint64 { return s.connects.Load() }

// SentCount returns the total number of successful frame APPENDs since
// startup. A batch of N frames counts as N (not 1) — this is the
// stream-frame metric, not the IMAP-command metric. Use BatchCount for
// the latter.
func (s *Sender) SentCount() uint64 { return s.sentCount.Load() }

// BatchCount returns the total number of APPEND commands issued
// (regardless of batch size). The ratio SentCount / BatchCount is
// the average batch size — a useful efficiency metric.
func (s *Sender) BatchCount() uint64 { return s.batchCount.Load() }

// Send enqueues one frame for APPEND and blocks until that frame's
// batch has been processed. Thread-safe. If Run has not yet started,
// the frame waits in the buffered queue; if Run has already exited
// (shutdown), Send returns the shutdown error.
func (s *Sender) Send(f protocol.Frame) error {
	req := &sendReq{frame: f, reply: make(chan error, 1)}
	s.queue <- req
	return <-req.reply
}

// Enqueue queues one frame for APPEND and returns once it has entered the
// bounded sender queue. The caller does not wait for the IMAP APPEND result.
// Used only for DATA fast-paths where throughput matters more than per-frame
// synchronous error reporting; queue capacity still provides back-pressure.
func (s *Sender) Enqueue(f protocol.Frame) error {
	s.queue <- &sendReq{frame: f}
	return nil
}

// CancelStream marks a stream closed so queued async DATA/FIN for it are
// discarded before APPEND. RST is still allowed through so peer notification is
// not suppressed.
func (s *Sender) CancelStream(streamID uint32) {
	if streamID == 0 {
		return
	}
	s.canceled.Store(streamID, time.Now().Add(streamCancelTTL))
}

// AllowStream clears a cancellation tombstone for a fresh stream lifecycle.
func (s *Sender) AllowStream(streamID uint32) {
	if streamID == 0 {
		return
	}
	s.canceled.Delete(streamID)
}

// connect ensures s.client is established. Caller must hold s.mu.
func (s *Sender) connect() error {
	if s.client != nil {
		return nil
	}
	if !s.lastFail.IsZero() && time.Since(s.lastFail) < s.failDelay {
		return fmt.Errorf("backoff: last fail %v ago, waiting %v",
			time.Since(s.lastFail).Round(time.Millisecond), s.failDelay)
	}

	c, err := dialClient(s.acc, nil)
	if err != nil {
		s.lastFail = time.Now()
		next := time.Duration(float64(s.failDelay) * s.cfg.ReconnectBackoffMultiplier())
		if next > s.cfg.ReconnectMaxDelay() {
			next = s.cfg.ReconnectMaxDelay()
		}
		s.failDelay = next
		return err
	}

	if err := ensureMailbox(c, s.acc.FolderSend); err != nil {
		tlog.Debugf("sender %s: ensure folder %q: %v",
			s.acc.Label(), s.acc.FolderSend, err)
	}

	s.client = c
	s.connected.Store(true)
	s.connects.Add(1)
	s.connectedAtNS.Store(time.Now().UnixNano())
	s.failDelay = s.cfg.ReconnectInitialDelay()
	tlog.Infof("sender %s: connected to %s", s.acc.Label(), s.acc.Host)
	return nil
}

// disconnect tears down the connection. Caller must hold s.mu.
func (s *Sender) disconnect() {
	if s.client == nil {
		return
	}
	_ = s.client.Logout().Wait()
	_ = s.client.Close()
	s.client = nil
	s.connected.Store(false)
}

// markFailed tears down the connection so the next cycle reconnects.
// Caller must hold s.mu.
func (s *Sender) markFailed(err error) {
	tlog.Warnf("sender %s: send failed: %v", s.acc.Label(), err)
	s.disconnect()
	s.lastFail = time.Now()
}

// Run drives the sender's queue-drain loop until ctx is cancelled. It
// also handles periodic NOOP keepalives so the IMAP server doesn't
// drop us for inactivity, and lazy reconnect after failure.
//
// During (re)connect, queued requests are NOT failed — they wait
// patiently until the connection is established or ctx is cancelled.
// Multipath callers that don't want to wait on a disconnected sender
// should consult Sender.Connected() first.
func (s *Sender) Run(ctx context.Context) {
	// IMAP servers typically time out idle connections at 30 minutes
	// (RFC 9051 §5.4). NOOP every ~10 minutes is comfortably within that.
	noop := time.NewTicker(10 * time.Minute)
	defer noop.Stop()

	for {
		// Step 1: ensure we have a connection. If not, wait on a
		// short retry timer (or cancellation) WITHOUT touching the
		// queue — requests that arrived during a disconnect must
		// be honoured once we come back up, not dropped.
		s.mu.Lock()
		if s.client == nil {
			err := s.connect()
			s.mu.Unlock()
			if err != nil {
				retry := time.NewTimer(500 * time.Millisecond)
				select {
				case <-ctx.Done():
					retry.Stop()
					s.drainPendingWithError(ctx.Err())
					return
				case <-retry.C:
				case <-noop.C:
				}
				continue
			}
		} else {
			s.mu.Unlock()
		}

		// Step 2: wait for at least one queued frame OR a keepalive
		// tick OR cancellation. Note: this select serves keepalives
		// only when the queue is empty — when traffic flows, NOOPs
		// piggyback by not being needed (every successful APPEND
		// also resets the server-side idle timer).
		var first *sendReq
		if len(s.pending) > 0 {
			select {
			case <-ctx.Done():
				s.drainPendingWithError(ctx.Err())
				s.mu.Lock()
				s.disconnect()
				s.mu.Unlock()
				return
			default:
			}
			first = s.pending[0]
			s.pending = s.pending[1:]
		} else {
			select {
			case <-ctx.Done():
				s.drainPendingWithError(ctx.Err())
				s.mu.Lock()
				s.disconnect()
				s.mu.Unlock()
				return
			case <-noop.C:
				s.keepalive()
				continue
			case first = <-s.queue:
			}
		}

		if s.dropIfCanceled(first) {
			continue
		}
		batch := []*sendReq{first}
		// If a batch delay is configured and no other frames are
		// already queued, pause briefly to let near-simultaneous
		// frames from other streams arrive. If the queue already has
		// work, skip the delay: batching pressure is already present.
		if d := s.cfg.BatchDelay(); d > 0 && len(s.pending) == 0 && len(s.queue) == 0 {
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				s.replyAll(batch, ctx.Err())
				s.drainPendingWithError(ctx.Err())
				s.mu.Lock()
				s.disconnect()
				s.mu.Unlock()
				return
			}
		}
		batch = s.opportunisticDrain(batch)
		s.sendBatch(batch)
	}
}

// opportunisticDrain pulls as many more requests as are immediately
// available from the queue, up to the configured limits. Non-blocking:
// returns as soon as the queue is empty or the limits are reached.
func (s *Sender) opportunisticDrain(batch []*sendReq) []*sendReq {
	maxFrames := s.cfg.BatchMaxFrames()
	maxBytes := s.cfg.BatchMaxBytes()
	clientID := batchClientID(batch[0].frame)
	// Approximate per-frame size — we don't pre-compute the exact
	// wire size because most frames are dominated by their payload.
	totalBytes := 0
	for _, r := range batch {
		totalBytes += len(r.frame.Payload) + protocol.HeaderSize
	}
	for len(batch) < maxFrames && totalBytes < maxBytes && len(s.pending) > 0 {
		r := s.pending[0]
		if s.dropIfCanceled(r) {
			s.pending = s.pending[1:]
			continue
		}
		if batchClientID(r.frame) != clientID {
			break
		}
		s.pending = s.pending[1:]
		batch = append(batch, r)
		totalBytes += len(r.frame.Payload) + protocol.HeaderSize
	}
	for len(batch) < maxFrames && totalBytes < maxBytes {
		select {
		case r := <-s.queue:
			if s.dropIfCanceled(r) {
				continue
			}
			if batchClientID(r.frame) != clientID {
				s.pending = append(s.pending, r)
				continue
			}
			batch = append(batch, r)
			totalBytes += len(r.frame.Payload) + protocol.HeaderSize
		default:
			return batch
		}
	}
	return batch
}

func batchClientID(f protocol.Frame) byte {
	return protocol.StreamClientID(f.StreamID)
}

// sendBatch APPENDs the given requests as a single IMAP message and
// dispatches the result to each request's reply channel. APPEND is retried
// because a dropped mobile/TLS/IMAP connection can make the final status
// ambiguous; duplicate frames are safer than silently losing DATA.
func (s *Sender) sendBatch(reqs []*sendReq) {
	reqs = s.filterCanceled(reqs)
	if len(reqs) == 0 {
		return
	}
	frames := make([]protocol.Frame, len(reqs))
	for i, r := range reqs {
		frames[i] = r.frame
	}

	var encoded []byte
	var err error
	if len(frames) == 1 {
		encoded = protocol.Encode(frames[0])
	} else {
		encoded, err = protocol.EncodeBatch(frames)
		if err != nil {
			s.replyAll(reqs, fmt.Errorf("encode batch: %w", err))
			return
		}
	}
	clientID := batchClientID(frames[0])
	wireBytes, err := s.keys.Encrypt(encoded, clientID)
	if err != nil {
		s.replyAll(reqs, fmt.Errorf("encrypt: %w", err))
		return
	}
	body, err := buildMessage(wireBytes, time.Now(), MessageOptions{
		Format:             s.cfg.EffectiveMessageFormat(),
		AttachmentFilename: s.cfg.EffectiveAttachmentFilename(),
		Subject:            s.cfg.EffectiveMessageSubject(),
		SubjectMode:        s.cfg.EffectiveMessageSubjectMode(),
		From:               s.acc.EffectiveMessageFrom(),
		To:                 s.cfg.EffectiveMessageTo(),
		ClientID:           clientID,
		SubjectClientID:    s.cfg.SubjectClientIDEnabled(),
	})
	if err != nil {
		s.replyAll(reqs, fmt.Errorf("build message: %w", err))
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	start := time.Now()
	opts := &imap.AppendOptions{
		Flags: []imap.Flag{imap.FlagDraft, imap.FlagSeen},
		Time:  time.Now(),
	}
	if err := s.appendWithRetryLocked(body, opts); err != nil {
		s.replyAll(reqs, err)
		return
	}

	s.sentCount.Add(uint64(len(reqs)))
	s.batchCount.Add(1)
	if tlog.Enabled(tlog.LevelTrace) {
		tlog.Tracef("sender %s: tx batch frames=%d body=%dB elapsed=%v (avg %dB/frame)",
			s.acc.Label(), len(reqs), len(body),
			time.Since(start).Round(time.Millisecond),
			len(body)/len(reqs))
	} else if tlog.Enabled(tlog.LevelDebug) && len(reqs) > 1 {
		tlog.Debugf("sender %s: batched %d frames into 1 APPEND (%dB)",
			s.acc.Label(), len(reqs), len(body))
	}

	s.replyAll(reqs, nil)
}

func (s *Sender) filterCanceled(reqs []*sendReq) []*sendReq {
	out := reqs[:0]
	for _, r := range reqs {
		if s.dropIfCanceled(r) {
			continue
		}
		out = append(out, r)
	}
	return out
}

func (s *Sender) dropIfCanceled(r *sendReq) bool {
	if r == nil || !dropAfterCancel(r.frame.Type) || r.frame.StreamID == 0 {
		return false
	}
	v, ok := s.canceled.Load(r.frame.StreamID)
	if !ok {
		return false
	}
	until, ok := v.(time.Time)
	if !ok || time.Now().After(until) {
		s.canceled.Delete(r.frame.StreamID)
		return false
	}
	if r.reply != nil {
		r.reply <- errStreamCanceled
	}
	return true
}

func dropAfterCancel(t byte) bool {
	switch t {
	case protocol.MsgData, protocol.MsgFin, protocol.MsgOpenOK, protocol.MsgOpenFail:
		return true
	default:
		return false
	}
}

func (s *Sender) appendWithRetryLocked(body []byte, opts *imap.AppendOptions) error {
	var lastErr error
	for attempt := 1; attempt <= appendMaxAttempts; attempt++ {
		if s.client == nil && s.appendHook == nil {
			if err := s.connect(); err != nil {
				lastErr = fmt.Errorf("connect: %w", err)
				tlog.Warnf("sender %s: APPEND retry connect attempt %d/%d failed: %v",
					s.acc.Label(), attempt, appendMaxAttempts, err)
				if attempt < appendMaxAttempts {
					s.lastFail = time.Time{}
				}
				s.waitBeforeAppendRetryLocked(attempt)
				continue
			}
		}

		if err := s.appendOnceLocked(body, opts); err != nil {
			lastErr = err
			tlog.Warnf("sender %s: APPEND attempt %d/%d failed: %v",
				s.acc.Label(), attempt, appendMaxAttempts, err)
			s.markFailed(err)
			if attempt < appendMaxAttempts {
				s.lastFail = time.Time{}
			}
			s.waitBeforeAppendRetryLocked(attempt)
			continue
		}

		if attempt > 1 {
			tlog.Infof("sender %s: APPEND retry succeeded on attempt %d/%d",
				s.acc.Label(), attempt, appendMaxAttempts)
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("sender %s not connected", s.acc.Label())
	}
	return fmt.Errorf("append failed after %d attempts: %w", appendMaxAttempts, lastErr)
}

func (s *Sender) waitBeforeAppendRetryLocked(attempt int) {
	if attempt >= appendMaxAttempts {
		return
	}
	time.Sleep(time.Duration(attempt) * appendRetryDelay)
}

func (s *Sender) appendOnceLocked(body []byte, opts *imap.AppendOptions) error {
	if s.appendHook != nil {
		return s.appendHook(body)
	}
	if s.client == nil {
		return fmt.Errorf("sender %s not connected", s.acc.Label())
	}

	cmd := s.client.Append(s.acc.FolderSend, int64(len(body)), opts)
	if _, err := cmd.Write(body); err != nil {
		return fmt.Errorf("append write: %w", err)
	}
	if err := cmd.Close(); err != nil {
		return fmt.Errorf("append close: %w", err)
	}
	if _, err := cmd.Wait(); err != nil {
		return fmt.Errorf("append wait: %w", err)
	}
	return nil
}

// keepalive runs a NOOP to refresh the server-side idle timer.
func (s *Sender) keepalive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		return
	}
	if err := s.client.Noop().Wait(); err != nil {
		tlog.Warnf("sender %s: NOOP failed: %v", s.acc.Label(), err)
		s.markFailed(err)
	}
}

// drainPendingWithError fails every still-queued request. Called on
// shutdown so Send callers don't block forever.
func (s *Sender) drainPendingWithError(err error) {
	if len(s.pending) > 0 {
		s.replyAll(s.pending, err)
		s.pending = nil
	}
	for {
		select {
		case r := <-s.queue:
			if r.reply != nil {
				r.reply <- err
			}
		default:
			return
		}
	}
}

// replyAll dispatches err (which may be nil) to every request's
// reply channel.
func (s *Sender) replyAll(reqs []*sendReq, err error) {
	for _, r := range reqs {
		if r.reply != nil {
			r.reply <- err
		} else if err != nil && s.AsyncErrorHandler != nil {
			go s.AsyncErrorHandler(r.frame, err)
		}
	}
}
