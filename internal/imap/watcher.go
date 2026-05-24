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

// FrameHandler is called for each frame decoded from an incoming message.
// It must be safe for concurrent use across watchers.
type FrameHandler func(f protocol.Frame, sourceAccount string)

// FrameFilter decides whether the watcher should process a given frame.
//
// When the filter returns false, the message that carried the frame is
// LEFT in the folder unmodified. This is the multi-client primitive:
// each client only processes (and deletes) the messages addressed to
// it, leaving messages destined for other clients in place for them to
// pick up.
//
// A nil filter accepts every frame (the single-client / server default).
type FrameFilter func(f protocol.Frame) bool

// Watcher owns one IMAP connection used exclusively for receiving frames.
// It selects FolderRecv, runs an IDLE loop, fetches new messages on
// notification, dispatches owned frames, and deletes only the messages
// it owns.
//
// On any failure the connection is closed and Run reconnects with
// exponential backoff.
type Watcher struct {
	acc     *config.AccountConfig
	cfg     *config.Config
	handler FrameHandler
	aead    *titcrypto.AEAD

	// Filter, if set, is consulted for every fetched message. Frames it
	// rejects are left in the folder.
	Filter FrameFilter

	notify chan struct{}
	kick   chan struct{}

	mu      sync.Mutex
	client  *imapclient.Client
	lastUID imap.UID

	// pendingExpunge accumulates UIDs marked \Deleted that have not
	// been EXPUNGEd yet. We batch EXPUNGE to avoid paying its
	// round-trip on every single cycle — for interactive workloads
	// (one keystroke per cycle) that single RTT dominates end-to-end
	// latency. The pending set is flushed when pendingCount reaches
	// LazyExpungeThreshold, when LazyExpungeMaxAge has elapsed, or on
	// shutdown.
	pendingExpunge imap.UIDSet
	pendingCount   int
	lastExpunge    time.Time

	// activeUntilNS holds the wall-clock (UnixNano) until which the
	// watcher polls at ActivePollInterval. Read/written atomically so
	// other goroutines (notably Sender → Tunnel → Kick) don't need a
	// mutex to extend the active window.
	activeUntilNS atomic.Int64

	// framesReceived counts frames the watcher has successfully
	// decoded and handed to the dispatcher. Used by Multipath to
	// pick "proven" accounts: an account whose watcher has delivered
	// at least one frame is healthy in BOTH directions (we APPENDed,
	// the peer received and responded). Reset to 0 on reconnect.
	framesReceived atomic.Uint64

	// lastFrameAtNS is the UnixNano timestamp of the most recent
	// successfully-decoded frame. Unlike framesReceived this counter
	// is NOT reset on reconnect — it gives Multipath a "recently
	// proven" signal that survives the brief gap between a session
	// failure and the next successful connect, so the "proven" routing
	// tier doesn't flap every time backoff fires.
	lastFrameAtNS atomic.Int64

	// startupCleanupDone is intentionally per Watcher lifecycle, not per
	// IMAP connection. Repeating cleanup after a reconnect can delete live
	// DATA that arrived while the watcher was reconnecting.
	startupCleanupDone bool
	receiveReady       atomic.Bool

	failDelay time.Duration
}

// NewWatcher constructs (but does not connect) a Watcher. aead may be
// nil to disable per-frame decryption.
func NewWatcher(cfg *config.Config, acc *config.AccountConfig, handler FrameHandler, aead *titcrypto.AEAD) *Watcher {
	return &Watcher{
		acc:       acc,
		cfg:       cfg,
		handler:   handler,
		aead:      aead,
		notify:    make(chan struct{}, 1),
		kick:      make(chan struct{}, 1),
		failDelay: cfg.ReconnectInitialDelay(),
	}
}

// Kick puts the watcher into active-polling mode for cfg.ActivePollDuration.
// Intended to be called when this side has just sent something and a
// response is likely. No-op in IDLE mode (the IDLE already wakes on
// every EXISTS). Safe for concurrent use.
func (w *Watcher) Kick() {
	w.activeUntilNS.Store(time.Now().Add(w.cfg.ActivePollDuration()).UnixNano())
	// Non-blocking signal — if a poll is already mid-sleep, wake it
	// immediately so it can switch to the active interval.
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// activeNow reports whether the watcher should currently poll at the
// active (fast) interval.
func (w *Watcher) activeNow() bool {
	until := w.activeUntilNS.Load()
	return until > 0 && time.Now().UnixNano() < until
}

// Label returns the account's label for logs.
func (w *Watcher) Label() string { return w.acc.Label() }

// FramesReceived returns the number of frames this watcher has
// successfully decoded and dispatched. Used by Multipath as the
// "proven account" signal — an account that has delivered at least
// one frame since (re)connect is known to work end-to-end. Reset
// to 0 on every reconnect (see Run).
func (w *Watcher) FramesReceived() uint64 { return w.framesReceived.Load() }

// ReceiveReady reports whether this watcher has selected the receive
// mailbox and established its lastUID baseline. Sending before this point can
// race a response into the startup snapshot and make that response look stale.
func (w *Watcher) ReceiveReady() bool { return w.receiveReady.Load() }

// LastFrameAt returns the wall-clock time of the most recent
// successfully-decoded frame, or the zero Time if none have been seen.
// Unlike FramesReceived this value persists across reconnects, so
// Multipath can keep an account in the "recently proven" tier during
// the brief gap while it reconnects.
func (w *Watcher) LastFrameAt() time.Time {
	ns := w.lastFrameAtNS.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// Run drives the watcher until ctx is cancelled. It reconnects on any
// error with exponential backoff bounded by ReconnectMaxDelay.
func (w *Watcher) Run(ctx context.Context) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		// Reset proven-counter at the start of each connection
		// attempt — if the peer reconfigured / went offline, we want
		// Multipath to re-prove this account before routing through
		// it again.
		w.framesReceived.Store(0)
		w.receiveReady.Store(false)
		err := w.runOnce(ctx)
		w.receiveReady.Store(false)
		if err != nil && !errors.Is(err, context.Canceled) {
			tlog.Warnf("watcher %s: %v (retrying in %v)",
				w.acc.Label(), err, w.failDelay)
		}
		// Try to flush any pending lazy expunges on a still-living
		// connection — best-effort. If the connection is broken the
		// flush is a no-op and a future best-effort cleanup can finish
		// the job.
		if err := w.flushPendingExpunge(); err != nil {
			tlog.Debugf("watcher %s: expunge flush on exit: %v",
				w.acc.Label(), err)
		}
		// Always tear down before retry.
		w.mu.Lock()
		if w.client != nil {
			_ = w.client.Logout().Wait()
			_ = w.client.Close()
			w.client = nil
		}
		w.mu.Unlock()

		if ctx.Err() != nil {
			return
		}

		// Sleep, with backoff.
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.failDelay):
		}
		next := time.Duration(float64(w.failDelay) * w.cfg.ReconnectBackoffMultiplier())
		if next > w.cfg.ReconnectMaxDelay() {
			next = w.cfg.ReconnectMaxDelay()
		}
		w.failDelay = next
	}
}

// runOnce establishes a connection and runs the IDLE loop until error
// or cancellation.
func (w *Watcher) runOnce(ctx context.Context) error {
	// Drain any stale notify signal from a previous run.
	select {
	case <-w.notify:
	default:
	}

	opts := &imapclient.Options{
		UnilateralDataHandler: &imapclient.UnilateralDataHandler{
			Mailbox: func(data *imapclient.UnilateralDataMailbox) {
				if data.NumMessages == nil {
					return
				}
				// Non-blocking signal; a single buffered slot is enough.
				select {
				case w.notify <- struct{}{}:
				default:
				}
			},
		},
	}

	c, err := dialClient(w.acc, opts)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.client = c
	w.mu.Unlock()

	if err := ensureMailbox(c, w.acc.FolderRecv); err != nil {
		tlog.Debugf("watcher %s: ensure folder %q: %v",
			w.acc.Label(), w.acc.FolderRecv, err)
	}

	sel, err := c.Select(w.acc.FolderRecv, nil).Wait()
	if err != nil {
		return fmt.Errorf("select %q: %w", w.acc.FolderRecv, err)
	}
	tlog.Infof("watcher %s: selected %q (%d messages, UIDNEXT=%d, UIDVALIDITY=%d)",
		w.acc.Label(), w.acc.FolderRecv, sel.NumMessages, sel.UIDNext, sel.UIDValidity)

	// Reset on success.
	w.failDelay = w.cfg.ReconnectInitialDelay()
	// Reset pending-expunge bookkeeping for the new session — UIDs
	// from a prior session refer to a different connection and may
	// even reference a different UIDVALIDITY.
	w.pendingExpunge = imap.UIDSet{}
	w.pendingCount = 0
	w.lastExpunge = time.Time{}

	// New messages will have UID >= UIDNEXT. Note: messages destined for
	// other clients that we LEFT in the folder may have UID < UIDNEXT,
	// but they're not ours so we don't need to fetch them anyway.
	if sel.UIDNext > 0 {
		w.lastUID = sel.UIDNext - 1
	}
	w.receiveReady.Store(true)
	// Clean leftover OWNED messages from before this watcher started, but
	// never on the hot receive connection. Fetching/deleting a large stale
	// folder can take many seconds on mobile IMAP providers; doing that here
	// would delay the first PONG/OPEN_OK and make Android look connected but
	// unusable. The cleanup is intentionally once per Watcher lifecycle only:
	// on IMAP reconnect, messages in the mailbox may be live traffic that
	// arrived while the watcher was reconnecting.
	if !w.startupCleanupDone && sel.NumMessages > 0 && w.lastUID > 0 {
		w.startStartupCleanup(ctx, w.lastUID, sel.UIDValidity, sel.NumMessages)
	}
	w.startupCleanupDone = true

	// Drain notify again — purge may have populated it via untagged EXISTS.
	select {
	case <-w.notify:
	default:
	}

	// Pick a wait strategy based on capabilities. IDLE (RFC 2177; folded
	// into IMAP4rev2) is what we want; some servers (e.g. seznam.cz)
	// don't implement it, so we fall back to NOOP polling at a
	// configured interval.
	caps := c.Caps()
	useIdle := caps.Has(imap.CapIdle) || caps.Has(imap.CapIMAP4rev2)
	if !useIdle {
		tlog.Infof("watcher %s: server does not advertise IDLE — falling back to NOOP polling every %v",
			w.acc.Label(), w.cfg.PollInterval())
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if useIdle {
			if err := w.waitIdle(ctx, c); err != nil {
				return err
			}
		} else {
			if err := w.waitPoll(ctx, c); err != nil {
				return err
			}
		}

		if err := w.fetchAndProcess(ctx); err != nil {
			return err
		}
	}
}

// waitIdle issues an IDLE command and blocks until an EXISTS-style
// untagged response arrives, the local tunnel kicks the watcher, a
// bounded poll interval elapses, or ctx is cancelled. The timeout keeps
// progress deterministic on IMAP servers that occasionally miss or
// coalesce EXISTS notifications while traffic is flowing.
func (w *Watcher) waitIdle(ctx context.Context, c *imapclient.Client) error {
	idleCmd, err := c.Idle()
	if err != nil {
		return fmt.Errorf("idle: %w", err)
	}

	interval := w.cfg.PollInterval()
	if w.activeNow() {
		interval = w.cfg.ActivePollInterval()
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()

	select {
	case <-w.notify:
	case <-w.kick:
	case <-timer.C:
	case <-ctx.Done():
		_ = idleCmd.Close()
		_ = idleCmd.Wait()
		return ctx.Err()
	}

	if err := idleCmd.Close(); err != nil {
		return fmt.Errorf("idle close: %w", err)
	}
	if err := idleCmd.Wait(); err != nil {
		return fmt.Errorf("idle wait: %w", err)
	}
	return nil
}

// waitPoll sleeps for the configured poll interval before returning to
// the main loop, which then runs FETCH directly on lastUID+1:*. We do
// NOT send a separate NOOP — FETCH on a non-existent UID range is well
// defined (RFC 9051) and returns just a tagged OK, so it's cheaper than
// NOOP+FETCH (saves one round-trip whenever there IS new data, costs
// the same when there isn't).
//
// The poll uses ActivePollInterval (default 100ms) while the watcher is
// in active mode, and PollInterval (default 3s) otherwise. Active mode
// is triggered by Kick() (called by the tunnel after each outbound
// APPEND) or by the previous poll having returned new frames.
//
// The poll terminates early if w.notify or w.kick is signalled
// mid-sleep.
func (w *Watcher) waitPoll(ctx context.Context, c *imapclient.Client) error {
	interval := w.cfg.PollInterval()
	if w.activeNow() {
		interval = w.cfg.ActivePollInterval()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.notify:
		// Server pushed an untagged EXISTS without us asking — process
		// it immediately.
	case <-w.kick:
		// Sender on this side just APPENDed — go check for a response
		// right away using the active interval. Drop the sleep entirely.
	case <-time.After(interval):
	}
	// Drain notify and kick — fetchAndProcess will pick up everything
	// by UID regardless.
	select {
	case <-w.notify:
	default:
	}
	select {
	case <-w.kick:
	default:
	}
	return nil
}

// processRange fetches a UID range, optionally dispatches owned frames,
// and deletes only the owned messages. Non-owned messages (filtered out)
// are LEFT in the folder for their intended recipient.
//
// Returns the highest UID seen (owned or not).
func (w *Watcher) processRange(ctx context.Context, uidSet imap.UIDSet, dispatch bool) (imap.UID, error) {
	w.mu.Lock()
	c := w.client
	w.mu.Unlock()
	if c == nil {
		return 0, errors.New("not connected")
	}

	fetchOpts := &imap.FetchOptions{
		UID:         true,
		BodySection: []*imap.FetchItemBodySection{{Peek: true}},
	}
	fetchStart := time.Now()
	fetchCmd := c.Fetch(uidSet, fetchOpts)
	defer fetchCmd.Close()

	var (
		owned       = imap.UIDSet{}
		maxUID      imap.UID
		ownedCount  int
		skipCount   int
		fetchedBody int
	)

	for {
		msg := fetchCmd.Next()
		if msg == nil {
			break
		}
		buf, err := msg.Collect()
		if err != nil {
			tlog.Warnf("watcher %s: fetch collect failed: %v",
				w.acc.Label(), err)
			continue
		}
		if buf.UID == 0 {
			continue
		}
		if buf.UID > maxUID {
			maxUID = buf.UID
		}
		body := findBodySectionBytes(buf.BodySection)
		if body == nil {
			tlog.Warnf("watcher %s: no body section for UID %d, skipping",
				w.acc.Label(), buf.UID)
			// Malformed message — treat as owned for cleanup purposes;
			// otherwise it would sit in the folder forever.
			owned.AddNum(buf.UID)
			continue
		}
		frames, malformed := w.decodeFrames(body, buf.UID)
		if malformed {
			owned.AddNum(buf.UID)
			continue
		}

		// Filter is checked on the first frame; the sender guarantees
		// every frame in one batch targets the same client ID.
		if w.Filter != nil && len(frames) > 0 && !w.Filter(frames[0]) {
			skipCount++
			continue
		}

		owned.AddNum(buf.UID)
		ownedCount++

		if !dispatch {
			continue
		}

		if tlog.Enabled(tlog.LevelTrace) && len(frames) > 1 {
			tlog.Tracef("watcher %s: rx batch UID=%d frames=%d",
				w.acc.Label(), buf.UID, len(frames))
		}

		for _, f := range frames {
			// Hand off a copy of the payload — the underlying buffer
			// is owned by the fetch.
			payloadCopy := make([]byte, len(f.Payload))
			copy(payloadCopy, f.Payload)
			f.Payload = payloadCopy

			if f.Type != protocol.MsgData {
				tlog.Debugf("watcher %s: rx %s stream=%d seq=%d payload=%dB",
					w.acc.Label(), protocol.TypeName(f.Type), f.StreamID, f.SeqID, len(f.Payload))
			} else if tlog.Enabled(tlog.LevelTrace) {
				tlog.Tracef("watcher %s: rx DATA stream=%d seq=%d payload=%dB",
					w.acc.Label(), f.StreamID, f.SeqID, len(f.Payload))
			}
			fetchedBody += len(f.Payload)
			w.framesReceived.Add(1)
			w.lastFrameAtNS.Store(time.Now().UnixNano())
			w.handler(f, w.acc.Label())
		}
	}
	if err := fetchCmd.Close(); err != nil {
		return maxUID, fmt.Errorf("fetch close: %w", err)
	}
	fetchElapsed := time.Since(fetchStart)
	if tlog.Enabled(tlog.LevelTrace) && (ownedCount > 0 || skipCount > 0) {
		tlog.Tracef("watcher %s: FETCH %s elapsed=%v owned=%d skipped=%d bytes=%d",
			w.acc.Label(), uidSet.String(),
			fetchElapsed.Round(time.Millisecond), ownedCount, skipCount, fetchedBody)
	}

	if ownedCount == 0 {
		if skipCount > 0 {
			tlog.Debugf("watcher %s: skipped %d non-owned message(s)",
				w.acc.Label(), skipCount)
		}
		return maxUID, nil
	}

	// Mark owned messages \Deleted. We do NOT issue EXPUNGE here on
	// every cycle — that would cost 1 IMAP round-trip every time, even
	// for a single keystroke through an SSH tunnel. Instead, deletions
	// are accumulated in pendingExpunge and flushed in batches by
	// expungePendingIfNeeded.
	//
	// STORE with Silent=true skips per-message FLAGS responses; we
	// still wait for the tagged OK before adding to the pending set so
	// we never expunge a UID the server didn't actually mark.
	storeFlags := &imap.StoreFlags{
		Op:     imap.StoreFlagsAdd,
		Silent: true,
		Flags:  []imap.Flag{imap.FlagDeleted},
	}
	storeCmd := c.Store(owned, storeFlags, nil)

	// Decide *before* waiting for STORE whether to also flush the
	// pending expunge in the same network round. When yes, both
	// commands are placed on the wire back-to-back; the server processes
	// them in order and we wait once at the end. That collapses what
	// would be two sequential round-trips into one.
	w.pendingExpunge.AddSet(owned)
	w.pendingCount += ownedCount
	shouldExpunge := w.shouldFlushPendingExpunge()
	var expungePending imap.UIDSet
	var expungeCmd *imapclient.ExpungeCommand
	useUIDExpunge := c.Caps().Has(imap.CapUIDPlus) || c.Caps().Has(imap.CapIMAP4rev2)
	if shouldExpunge {
		// Snapshot the current pending set; clear w.pendingExpunge so
		// later cycles don't double-expunge the same UIDs if we get
		// interleaved.
		expungePending = w.pendingExpunge
		w.pendingExpunge = imap.UIDSet{}
		w.pendingCount = 0
		w.lastExpunge = time.Now()
		if useUIDExpunge {
			expungeCmd = c.UIDExpunge(expungePending)
		} else {
			// Plain EXPUNGE purges every \Deleted message — works on
			// dedicated tunnel folders since we're the only writer.
			expungeCmd = c.Expunge()
		}
	}

	storeStart := time.Now()
	if err := storeCmd.Close(); err != nil {
		return maxUID, fmt.Errorf("store \\Deleted: %w", err)
	}
	storeElapsed := time.Since(storeStart)
	var expungeElapsed time.Duration
	if expungeCmd != nil {
		expungeStart := time.Now()
		if _, err := expungeCmd.Collect(); err != nil {
			return maxUID, fmt.Errorf("expunge: %w", err)
		}
		expungeElapsed = time.Since(expungeStart)
	}
	if tlog.Enabled(tlog.LevelTrace) {
		if expungeCmd != nil {
			tlog.Tracef("watcher %s: STORE+UIDEXPUNGE pipelined: store=%v expunge=%v owned=%d pending=%d (now flushed)",
				w.acc.Label(),
				storeElapsed.Round(time.Millisecond),
				expungeElapsed.Round(time.Millisecond),
				ownedCount, len(expungePending))
		} else {
			tlog.Tracef("watcher %s: STORE only (lazy expunge): elapsed=%v owned=%d pending=%d",
				w.acc.Label(),
				storeElapsed.Round(time.Millisecond),
				ownedCount, w.pendingCount)
		}
	}

	// Extend the active-poll window: we just got new data, so more is
	// likely soon.
	if dispatch && ownedCount > 0 {
		w.activeUntilNS.Store(time.Now().Add(w.cfg.ActivePollDuration()).UnixNano())
	}
	if skipCount > 0 {
		tlog.Debugf("watcher %s: processed %d, skipped %d (other client)",
			w.acc.Label(), ownedCount, skipCount)
	}
	return maxUID, nil
}

// shouldFlushPendingExpunge returns true when the accumulated
// \Deleted-marked UIDs should be EXPUNGEd this cycle. Triggers:
//
//   - count >= LazyExpungeThreshold: keep the mailbox compact under
//     sustained traffic.
//   - elapsed >= LazyExpungeMaxAge: bound the staleness on bursty
//     workloads where the threshold takes a long time to reach.
func (w *Watcher) shouldFlushPendingExpunge() bool {
	if w.pendingCount == 0 {
		return false
	}
	if w.pendingCount >= w.cfg.LazyExpungeThreshold() {
		return true
	}
	if w.lastExpunge.IsZero() {
		// Don't let the very first cycle skip — install a baseline so
		// the age check is meaningful next time.
		w.lastExpunge = time.Now()
		return false
	}
	return time.Since(w.lastExpunge) >= w.cfg.LazyExpungeMaxAge()
}

// flushPendingExpunge forces an EXPUNGE of every UID currently pending,
// without waiting for the threshold or age timer. Intended for shutdown.
func (w *Watcher) flushPendingExpunge() error {
	if w.pendingCount == 0 {
		return nil
	}
	w.mu.Lock()
	c := w.client
	w.mu.Unlock()
	if c == nil {
		// Connection's gone; a future best-effort cleanup can finish the job.
		return nil
	}
	pending := w.pendingExpunge
	w.pendingExpunge = imap.UIDSet{}
	w.pendingCount = 0
	w.lastExpunge = time.Now()
	if c.Caps().Has(imap.CapUIDPlus) || c.Caps().Has(imap.CapIMAP4rev2) {
		if _, err := c.UIDExpunge(pending).Collect(); err != nil {
			return fmt.Errorf("uid expunge: %w", err)
		}
	} else {
		if _, err := c.Expunge().Collect(); err != nil {
			return fmt.Errorf("expunge: %w", err)
		}
	}
	return nil
}

// fetchAndProcess fetches messages with UID > lastUID, dispatches owned
// frames, and deletes the owned messages. Non-owned messages (multi-
// client) are left in place. lastUID is advanced past every UID seen,
// owned or not.
//
// IMPORTANT: the upper bound is a literal max-UID (0xFFFFFFFF), not the
// IMAP "*" sentinel. RFC 9051 §9 says "3291:* includes the UID of the
// last message in the mailbox even if that value is less than 3291" —
// i.e. an empty result is impossible if any message exists. With our
// lazy-expunge strategy the mailbox always carries some \Deleted
// messages, so "*" would re-fetch them every time the watcher woke up
// without new mail (e.g. from a kick after this side sent something).
// Using a numeric upper bound suppresses the RFC-mandated range swap.
func (w *Watcher) fetchAndProcess(ctx context.Context) error {
	uidSet := imap.UIDSet{}
	uidSet.AddRange(w.lastUID+1, 0xFFFFFFFF)
	maxUID, err := w.processRange(ctx, uidSet, true)
	if err != nil {
		return err
	}
	if maxUID > w.lastUID {
		w.lastUID = maxUID
	}
	return nil
}

func (w *Watcher) startStartupCleanup(ctx context.Context, maxUID imap.UID, uidValidity uint32, numMessages uint32) {
	tlog.Infof("watcher %s: startup cleanup scheduled for %d existing message(s) up to UID %d",
		w.acc.Label(), numMessages, maxUID)
	go func() {
		if err := w.cleanupStartupMessages(ctx, maxUID, uidValidity); err != nil {
			if ctx.Err() == nil {
				tlog.Warnf("watcher %s: startup cleanup failed: %v",
					w.acc.Label(), err)
			}
			return
		}
	}()
}

func (w *Watcher) cleanupStartupMessages(ctx context.Context, maxUID imap.UID, uidValidity uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c, err := dialClient(w.acc, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = c.Logout().Wait()
		_ = c.Close()
	}()

	if err := ensureMailbox(c, w.acc.FolderRecv); err != nil {
		tlog.Debugf("watcher %s: cleanup ensure folder %q: %v",
			w.acc.Label(), w.acc.FolderRecv, err)
	}

	sel, err := c.Select(w.acc.FolderRecv, nil).Wait()
	if err != nil {
		return fmt.Errorf("cleanup select %q: %w", w.acc.FolderRecv, err)
	}
	if sel.UIDValidity != uidValidity {
		return fmt.Errorf("cleanup skipped: UIDVALIDITY changed from %d to %d",
			uidValidity, sel.UIDValidity)
	}

	uidSet := imap.UIDSet{}
	uidSet.AddRange(1, maxUID)
	start := time.Now()
	owned, ownedCount, skippedCount, err := w.findOwnedUIDs(c, uidSet)
	if err != nil {
		return err
	}
	if ownedCount == 0 {
		if skippedCount > 0 {
			tlog.Debugf("watcher %s: startup cleanup skipped %d non-owned old message(s)",
				w.acc.Label(), skippedCount)
		}
		tlog.Infof("watcher %s: startup cleanup found no owned old messages (elapsed=%v)",
			w.acc.Label(), time.Since(start).Round(time.Millisecond))
		return nil
	}

	storeFlags := &imap.StoreFlags{
		Op:     imap.StoreFlagsAdd,
		Silent: true,
		Flags:  []imap.Flag{imap.FlagDeleted},
	}
	if err := c.Store(owned, storeFlags, nil).Close(); err != nil {
		return fmt.Errorf("cleanup store \\Deleted: %w", err)
	}
	if c.Caps().Has(imap.CapUIDPlus) || c.Caps().Has(imap.CapIMAP4rev2) {
		if _, err := c.UIDExpunge(owned).Collect(); err != nil {
			return fmt.Errorf("cleanup uid expunge: %w", err)
		}
	} else {
		if _, err := c.Expunge().Collect(); err != nil {
			return fmt.Errorf("cleanup expunge: %w", err)
		}
	}
	tlog.Infof("watcher %s: startup cleanup removed %d old owned message(s) (skipped=%d elapsed=%v)",
		w.acc.Label(), ownedCount, skippedCount, time.Since(start).Round(time.Millisecond))
	return nil
}

func (w *Watcher) findOwnedUIDs(c *imapclient.Client, uidSet imap.UIDSet) (imap.UIDSet, int, int, error) {
	fetchOpts := &imap.FetchOptions{
		UID:         true,
		BodySection: []*imap.FetchItemBodySection{{Peek: true}},
	}
	fetchCmd := c.Fetch(uidSet, fetchOpts)
	defer fetchCmd.Close()

	owned := imap.UIDSet{}
	ownedCount := 0
	skippedCount := 0
	for {
		msg := fetchCmd.Next()
		if msg == nil {
			break
		}
		buf, err := msg.Collect()
		if err != nil {
			tlog.Warnf("watcher %s: cleanup fetch collect failed: %v",
				w.acc.Label(), err)
			continue
		}
		if buf.UID == 0 {
			continue
		}
		body := findBodySectionBytes(buf.BodySection)
		if body == nil {
			tlog.Warnf("watcher %s: cleanup no body section for UID %d, deleting as malformed",
				w.acc.Label(), buf.UID)
			owned.AddNum(buf.UID)
			ownedCount++
			continue
		}
		frames, malformed := w.decodeFrames(body, buf.UID)
		if malformed {
			owned.AddNum(buf.UID)
			ownedCount++
			continue
		}
		if w.Filter != nil && len(frames) > 0 && !w.Filter(frames[0]) {
			skippedCount++
			continue
		}
		owned.AddNum(buf.UID)
		ownedCount++
	}
	if err := fetchCmd.Close(); err != nil {
		return owned, ownedCount, skippedCount, fmt.Errorf("cleanup fetch close: %w", err)
	}
	return owned, ownedCount, skippedCount, nil
}

func (w *Watcher) decodeFrames(body []byte, uid imap.UID) ([]protocol.Frame, bool) {
	frameBytes, err := extractFrame(body)
	if err != nil {
		tlog.Warnf("watcher %s: extract frame UID %d: %v",
			w.acc.Label(), uid, err)
		return nil, true
	}
	// Decrypt if encryption is configured. AEAD.Decrypt is a no-op when
	// w.aead is nil. A decrypt failure most often means the two sides have
	// mismatched encryption_passphrase config — log and treat the message as
	// malformed.
	frameBytes, err = w.aead.Decrypt(frameBytes)
	if err != nil {
		tlog.Warnf("watcher %s: decrypt frame UID %d: %v (encryption_passphrase mismatch?)",
			w.acc.Label(), uid, err)
		return nil, true
	}
	// Decode either a single frame or a batch envelope. The two formats are
	// distinguishable by the leading byte: a batch envelope starts with the
	// BatchMagic sentinel (0xBA) which is not a valid frame type.
	if protocol.IsBatch(frameBytes) {
		frames, err := protocol.DecodeBatch(frameBytes)
		if err != nil {
			tlog.Warnf("watcher %s: decode batch UID %d: %v",
				w.acc.Label(), uid, err)
			return nil, true
		}
		return frames, false
	}
	f, err := protocol.Decode(frameBytes)
	if err != nil {
		tlog.Warnf("watcher %s: decode frame UID %d: %v",
			w.acc.Label(), uid, err)
		return nil, true
	}
	return []protocol.Frame{f}, false
}

func findBodySectionBytes(sections []imapclient.FetchBodySectionBuffer) []byte {
	for _, s := range sections {
		// We requested a single BodySection with no Part/Specifier — match
		// permissively against the first such response.
		return s.Bytes
	}
	return nil
}
