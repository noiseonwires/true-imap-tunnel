package imap

import (
	"errors"
	"testing"
	"time"

	"github.com/noiseonwires/true-imap-tunnel/internal/config"
	"github.com/noiseonwires/true-imap-tunnel/internal/protocol"
)

func TestOpportunisticDrainKeepsBatchesClientSpecific(t *testing.T) {
	s := testSender()

	first := testReq(protocol.MsgData, protocol.MakeStreamID(1, 1))
	s.queue <- testReq(protocol.MsgData, protocol.MakeStreamID(2, 1))
	s.queue <- testReq(protocol.MsgData, protocol.MakeStreamID(1, 2))
	s.queue <- testReq(protocol.MsgPong, protocol.MakeStreamID(1, 0))

	batch := s.opportunisticDrain([]*sendReq{first})
	if len(batch) != 3 {
		t.Fatalf("batch length = %d, want 3", len(batch))
	}
	for _, r := range batch {
		if got := protocol.StreamClientID(r.frame.StreamID); got != 1 {
			t.Fatalf("batched client ID = %d, want 1", got)
		}
	}
	if len(s.pending) != 1 {
		t.Fatalf("pending length = %d, want 1", len(s.pending))
	}
	if got := protocol.StreamClientID(s.pending[0].frame.StreamID); got != 2 {
		t.Fatalf("pending client ID = %d, want 2", got)
	}
	if len(s.queue) != 0 {
		t.Fatalf("queue length = %d, want 0", len(s.queue))
	}
}

func TestOpportunisticDrainDoesNotMixUntaggedControlWithTaggedClient(t *testing.T) {
	s := testSender()

	first := testReq(protocol.MsgPing, 0)
	s.queue <- testReq(protocol.MsgData, protocol.MakeStreamID(1, 1))

	batch := s.opportunisticDrain([]*sendReq{first})
	if len(batch) != 1 {
		t.Fatalf("batch length = %d, want 1", len(batch))
	}
	if len(s.pending) != 1 {
		t.Fatalf("pending length = %d, want 1", len(s.pending))
	}
	if got := protocol.StreamClientID(s.pending[0].frame.StreamID); got != 1 {
		t.Fatalf("pending client ID = %d, want 1", got)
	}
}

func TestOpportunisticDrainPreservesPendingOrderOnClientMismatch(t *testing.T) {
	s := testSender()

	first := testReq(protocol.MsgData, protocol.MakeStreamID(1, 1))
	s.pending = []*sendReq{
		testReq(protocol.MsgData, protocol.MakeStreamID(2, 1)),
		testReq(protocol.MsgData, protocol.MakeStreamID(2, 2)),
	}

	batch := s.opportunisticDrain([]*sendReq{first})
	if len(batch) != 1 {
		t.Fatalf("batch length = %d, want 1", len(batch))
	}
	if len(s.pending) != 2 {
		t.Fatalf("pending length = %d, want 2", len(s.pending))
	}
	for i, wantLocalID := range []uint32{1, 2} {
		if got := s.pending[i].frame.StreamID & protocol.StreamLocalIDMask; got != wantLocalID {
			t.Fatalf("pending[%d] local stream ID = %d, want %d", i, got, wantLocalID)
		}
	}
}

func TestDrainPendingWithErrorIncludesDeferredRequests(t *testing.T) {
	wantErr := errors.New("stop")
	s := testSender()
	pending := []*sendReq{
		{reply: make(chan error, 1)},
		{reply: make(chan error, 1)},
	}
	s.pending = pending

	s.drainPendingWithError(wantErr)

	if len(s.pending) != 0 {
		t.Fatalf("pending length = %d, want 0", len(s.pending))
	}
	for i, r := range pending {
		select {
		case err := <-r.reply:
			if !errors.Is(err, wantErr) {
				t.Fatalf("pending[%d] error = %v, want %v", i, err, wantErr)
			}
		default:
			t.Fatalf("pending[%d] reply was not signaled", i)
		}
	}
}

func TestSendBatchRetriesAppendBeforeSuccess(t *testing.T) {
	s := testSender()
	attempts := 0
	s.appendHook = func([]byte) error {
		attempts++
		if attempts < appendMaxAttempts {
			return errors.New("temporary append failure")
		}
		return nil
	}

	r := testReq(protocol.MsgData, protocol.MakeStreamID(1, 1))
	r.reply = make(chan error, 1)
	s.sendBatch([]*sendReq{r})

	if attempts != appendMaxAttempts {
		t.Fatalf("attempts = %d, want %d", attempts, appendMaxAttempts)
	}
	select {
	case err := <-r.reply:
		if err != nil {
			t.Fatalf("reply error = %v, want nil", err)
		}
	default:
		t.Fatal("reply was not signaled")
	}
	if got := s.SentCount(); got != 1 {
		t.Fatalf("SentCount = %d, want 1", got)
	}
	if got := s.BatchCount(); got != 1 {
		t.Fatalf("BatchCount = %d, want 1", got)
	}
}

func TestSendBatchReportsFailureAfterAppendRetries(t *testing.T) {
	s := testSender()
	attempts := 0
	wantErr := errors.New("append still broken")
	s.appendHook = func([]byte) error {
		attempts++
		return wantErr
	}

	r := testReq(protocol.MsgData, protocol.MakeStreamID(1, 1))
	r.reply = make(chan error, 1)
	s.sendBatch([]*sendReq{r})

	if attempts != appendMaxAttempts {
		t.Fatalf("attempts = %d, want %d", attempts, appendMaxAttempts)
	}
	select {
	case err := <-r.reply:
		if !errors.Is(err, wantErr) {
			t.Fatalf("reply error = %v, want wrapping %v", err, wantErr)
		}
	default:
		t.Fatal("reply was not signaled")
	}
	if got := s.SentCount(); got != 0 {
		t.Fatalf("SentCount = %d, want 0", got)
	}
}

func TestSendBatchDropsCanceledDataButAllowsRst(t *testing.T) {
	s := testSender()
	streamID := uint32(42)
	s.CancelStream(streamID)

	appends := 0
	s.appendHook = func([]byte) error {
		appends++
		return nil
	}

	data := testReq(protocol.MsgData, streamID)
	data.reply = make(chan error, 1)
	rst := testReq(protocol.MsgRst, streamID)
	rst.reply = make(chan error, 1)

	s.sendBatch([]*sendReq{data, rst})

	select {
	case err := <-data.reply:
		if !errors.Is(err, errStreamCanceled) {
			t.Fatalf("DATA error = %v, want %v", err, errStreamCanceled)
		}
	default:
		t.Fatal("DATA reply was not signaled")
	}
	select {
	case err := <-rst.reply:
		if err != nil {
			t.Fatalf("RST error = %v, want nil", err)
		}
	default:
		t.Fatal("RST reply was not signaled")
	}
	if appends != 1 {
		t.Fatalf("appends = %d, want 1", appends)
	}
	if got := s.SentCount(); got != 1 {
		t.Fatalf("SentCount = %d, want 1", got)
	}
}

func testSender() *Sender {
	return &Sender{
		acc: &config.AccountConfig{
			Name:       "test",
			FolderSend: "out",
		},
		cfg: &config.Config{
			BatchMaxFrames_: 8,
			BatchMaxKB:      1,
		},
		queue: make(chan *sendReq, 8),
		done:  make(chan struct{}),
	}
}

func testReq(typ byte, streamID uint32) *sendReq {
	return &sendReq{frame: protocol.Frame{Type: typ, StreamID: streamID}}
}

func TestSendAndEnqueueFailFastAfterStop(t *testing.T) {
	s := testSender()
	close(s.done) // simulate Run having exited

	f := protocol.Frame{Type: protocol.MsgData, StreamID: protocol.MakeStreamID(1, 1)}
	if err := s.Send(f); !errors.Is(err, errSenderStopped) {
		t.Fatalf("Send after stop = %v, want %v", err, errSenderStopped)
	}
	if err := s.Enqueue(f); !errors.Is(err, errSenderStopped) {
		t.Fatalf("Enqueue after stop = %v, want %v", err, errSenderStopped)
	}
	if got := len(s.queue); got != 0 {
		t.Fatalf("queue length = %d, want 0 (nothing enqueued after stop)", got)
	}
}

func TestSendReturnsRealReplyNotShutdownError(t *testing.T) {
	s := testSender()
	wantErr := errors.New("real append result")

	// Stand-in for Run: take the request, deliver the real reply, then
	// close done. Send must surface the real reply, never dropping it in
	// favour of the shutdown error.
	go func() {
		req := <-s.queue
		req.reply <- wantErr
		close(s.done)
	}()

	f := protocol.Frame{Type: protocol.MsgData, StreamID: protocol.MakeStreamID(1, 1)}
	if err := s.Send(f); !errors.Is(err, wantErr) {
		t.Fatalf("Send = %v, want %v", err, wantErr)
	}
}

func TestSweepCanceledEvictsExpiredTombstones(t *testing.T) {
	s := testSender()
	now := time.Now()

	// Expired tombstones (deadline already passed) must be evicted.
	for i := uint32(1); i <= 100; i++ {
		s.canceled.Store(i, now.Add(-time.Minute))
	}
	// Fresh tombstones (deadline in the future) must survive.
	fresh := map[uint32]bool{}
	for i := uint32(1000); i < 1005; i++ {
		s.canceled.Store(i, now.Add(time.Minute))
		fresh[i] = true
	}

	s.sweepCanceled(now)

	remaining := 0
	s.canceled.Range(func(key, _ any) bool {
		id, _ := key.(uint32)
		if !fresh[id] {
			t.Errorf("expired tombstone %v survived sweep", key)
		}
		remaining++
		return true
	})
	if remaining != len(fresh) {
		t.Fatalf("remaining tombstones = %d, want %d", remaining, len(fresh))
	}
}
