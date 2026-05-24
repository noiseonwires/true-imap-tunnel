package imap

import (
	"errors"
	"testing"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/config"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/protocol"
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
	}
}

func testReq(typ byte, streamID uint32) *sendReq {
	return &sendReq{frame: protocol.Frame{Type: typ, StreamID: streamID}}
}
