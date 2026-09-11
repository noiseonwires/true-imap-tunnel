package imap

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
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

func TestSendCancellationWhileQueueFull(t *testing.T) {
	sender := testSender()
	for count := 0; count < cap(sender.queue); count++ {
		sender.queue <- testReq(protocol.MsgData, 1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- sender.SendContext(ctx, protocol.Frame{Type: protocol.MsgData, StreamID: 2}) }()
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("queue cancellation blocked")
	}
	if len(sender.queue) != cap(sender.queue) {
		t.Fatal("canceled send changed full queue")
	}
}

func TestBatchDrainPreservesDeferredClientFIFO(t *testing.T) {
	sender := testSender()
	first := testReq(protocol.MsgData, protocol.MakeStreamID(1, 1))
	older := testReq(protocol.MsgData, protocol.MakeStreamID(1, 2))
	newer := testReq(protocol.MsgData, protocol.MakeStreamID(1, 3))
	other := testReq(protocol.MsgData, protocol.MakeStreamID(2, 1))
	sender.pending = []*sendReq{other, older}
	sender.queue <- newer
	batch := sender.opportunisticDrain([]*sendReq{first})
	if len(batch) != 3 || batch[1] != older || batch[2] != newer || len(sender.pending) != 1 || sender.pending[0] != other {
		t.Fatal("deferred FIFO or client isolation violated")
	}
}

func TestBatchDrainYieldsFairly(t *testing.T) {
	sender := testSender()
	first := testReq(protocol.MsgData, protocol.MakeStreamID(1, 1))
	second := testReq(protocol.MsgData, protocol.MakeStreamID(2, 1))
	third := testReq(protocol.MsgData, protocol.MakeStreamID(3, 1))
	sender.pending = []*sendReq{second, third}
	for count := 0; count < cap(sender.queue); count++ {
		sender.queue <- testReq(protocol.MsgData, protocol.MakeStreamID(1, uint32(count+2)))
	}
	_ = sender.opportunisticDrain([]*sendReq{first})
	for _, want := range []*sendReq{second, third} {
		if len(sender.pending) == 0 || sender.pending[0] != want {
			t.Fatal("busy client starved another client")
		}
		sender.pending[0] = nil
		sender.pending = sender.pending[1:]
		for len(sender.queue) < cap(sender.queue) {
			sender.queue <- testReq(protocol.MsgData, protocol.MakeStreamID(1, 50))
		}
		batch := sender.opportunisticDrain([]*sendReq{want})
		if batch[0] != want {
			t.Fatal("queued client did not get service")
		}
	}
}

func TestAsyncBatchFailureCompletesEveryRequest(t *testing.T) {
	sender := testSender()
	sender.appendHook = func([]byte) error { return errors.New("append unavailable") }
	completed := make(map[uint32]int)
	failed := make(map[uint32]int)
	sender.AsyncErrorHandler = func(frame protocol.Frame, err error) {
		if err == nil {
			t.Error("missing async error")
		}
		failed[frame.StreamID]++
	}
	var requests []*sendReq
	for id := uint32(1); id <= 3; id++ {
		requests = append(requests, &sendReq{frame: protocol.Frame{Type: protocol.MsgData, StreamID: id}, complete: func(err error) {
			if err == nil {
				t.Error("missing completion error")
			}
			completed[id]++
		}})
	}
	sender.sendBatch(requests)
	for id := uint32(1); id <= 3; id++ {
		if completed[id] != 1 || failed[id] != 1 {
			t.Fatalf("stream %d completion=%d failure=%d", id, completed[id], failed[id])
		}
	}
}

func TestDeferredClientBurstDrainsWithoutLoss(t *testing.T) {
	sender := testSender()
	var expected []*sendReq
	for index := 0; index < 1000; index++ {
		request := testReq(protocol.MsgData, protocol.MakeStreamID(byte(index%8+1), uint32(index/8+1)))
		expected = append(expected, request)
		sender.pending = append(sender.pending, request)
	}
	seen := make(map[*sendReq]bool)
	last := make(map[byte]uint32)
	for len(sender.pending) > 0 {
		first := sender.pending[0]
		sender.pending[0] = nil
		sender.pending = sender.pending[1:]
		batch := sender.opportunisticDrain([]*sendReq{first})
		clientID := batchClientID(first.frame)
		for _, request := range batch {
			id := request.frame.StreamID & protocol.StreamLocalIDMask
			if seen[request] || batchClientID(request.frame) != clientID || id <= last[clientID] {
				t.Fatal("duplicate, mixed client, or reordered burst")
			}
			seen[request], last[clientID] = true, id
		}
	}
	if len(seen) != len(expected) {
		t.Fatalf("drained %d want %d", len(seen), len(expected))
	}
}

func TestQueuedCanceledRequestIsNotAppended(t *testing.T) {
	sender := testSender()
	ctx, cancel := context.WithCancel(context.Background())
	completed := make(chan error, 1)
	if err := sender.EnqueueContext(ctx, protocol.Frame{Type: protocol.MsgData, StreamID: 1}, func(err error) { completed <- err }); err != nil {
		t.Fatal(err)
	}
	cancel()
	sender.appendHook = func([]byte) error { t.Error("canceled request appended"); return nil }
	sender.sendBatch([]*sendReq{<-sender.queue})
	if err := <-completed; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSenderCancellationDuringIMAPOperation(t *testing.T) {
	for _, phase := range []string{"greeting", "login", "continuation", "append-result"} {
		t.Run(phase, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			reached := make(chan struct{})
			peerDone := make(chan struct{})
			go func() {
				defer close(peerDone)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				reader := bufio.NewReader(conn)
				stall := func() { close(reached); _, _ = io.Copy(io.Discard, reader) }
				if phase == "greeting" {
					stall()
					return
				}
				_, _ = io.WriteString(conn, "* OK [CAPABILITY IMAP4rev1] ready\r\n")
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					fields := strings.Fields(line)
					if len(fields) < 2 {
						return
					}
					switch fields[1] {
					case "LOGIN":
						if phase == "login" {
							stall()
							return
						}
					case "APPEND":
						if phase == "continuation" {
							stall()
							return
						}
						var size int
						_, _ = fmt.Sscanf(line[strings.LastIndex(line, "{"):], "{%d}", &size)
						_, _ = io.WriteString(conn, "+ continue\r\n")
						_, _ = io.CopyN(io.Discard, reader, int64(size))
						_, _ = reader.ReadString('\n')
						stall()
						return
					case "CAPABILITY":
						_, _ = io.WriteString(conn, "* CAPABILITY IMAP4rev1\r\n")
					}
					_, _ = fmt.Fprintf(conn, "%s OK done\r\n", fields[0])
				}
			}()
			sender := NewSender(&config.Config{}, &config.AccountConfig{Host: listener.Addr().String(), TLS: "none", Username: "test", Password: "test", FolderSend: "out"}, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go sender.Run(ctx)
			if err := sender.Enqueue(protocol.Frame{Type: protocol.MsgData, StreamID: 1, Payload: []byte("data")}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-reached:
			case <-time.After(3 * time.Second):
				t.Fatal("phase not reached")
			}
			cancel()
			select {
			case <-sender.done:
			case <-time.After(time.Second):
				t.Fatal("sender did not cancel")
			}
			select {
			case <-peerDone:
			case <-time.After(time.Second):
				t.Fatal("socket did not close")
			}
		})
	}
}
