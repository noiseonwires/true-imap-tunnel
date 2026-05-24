package streams

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/protocol"
)

// TestReorderBuffer verifies that frames delivered out of order are
// handed to the user callback in SeqID order.
func TestReorderBuffer(t *testing.T) {
	m := NewManager(func(f protocol.Frame) error { return nil })
	m.Reorder = true

	var got []uint32
	var mu sync.Mutex
	deliver := func(f protocol.Frame) {
		mu.Lock()
		got = append(got, f.SeqID)
		mu.Unlock()
	}

	frames := []protocol.Frame{
		{StreamID: 1, SeqID: 3, Type: protocol.MsgData, Payload: []byte("c")},
		{StreamID: 1, SeqID: 1, Type: protocol.MsgData, Payload: []byte("a")},
		{StreamID: 1, SeqID: 2, Type: protocol.MsgData, Payload: []byte("b")},
		{StreamID: 1, SeqID: 5, Type: protocol.MsgData, Payload: []byte("e")},
		{StreamID: 1, SeqID: 4, Type: protocol.MsgData, Payload: []byte("d")},
	}
	for _, f := range frames {
		m.DispatchFrame(f, deliver)
	}

	want := []uint32{1, 2, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("len: got %d want %d (got=%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("position %d: got %d want %d (got=%v)", i, got[i], want[i], got)
		}
	}
}

func TestDispatchFrameSerializesHandlersPerStream(t *testing.T) {
	m := NewManager(func(f protocol.Frame) error { return nil })
	m.Reorder = true

	seq1Started := make(chan struct{})
	releaseSeq1 := make(chan struct{})
	seq2Delivered := make(chan struct{})
	done1 := make(chan struct{})
	done2 := make(chan struct{})

	deliver := func(f protocol.Frame) {
		switch f.SeqID {
		case 1:
			close(seq1Started)
			<-releaseSeq1
		case 2:
			close(seq2Delivered)
		}
	}

	go func() {
		defer close(done1)
		m.DispatchFrame(protocol.Frame{StreamID: 1, SeqID: 1, Type: protocol.MsgData}, deliver)
	}()

	select {
	case <-seq1Started:
	case <-time.After(time.Second):
		t.Fatal("seq=1 handler did not start")
	}

	go func() {
		defer close(done2)
		m.DispatchFrame(protocol.Frame{StreamID: 1, SeqID: 2, Type: protocol.MsgData}, deliver)
	}()

	select {
	case <-seq2Delivered:
		t.Fatal("seq=2 handler ran before seq=1 handler returned")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseSeq1)

	select {
	case <-done1:
	case <-time.After(time.Second):
		t.Fatal("seq=1 dispatch did not finish")
	}
	select {
	case <-seq2Delivered:
	case <-time.After(time.Second):
		t.Fatal("seq=2 handler did not run")
	}
	select {
	case <-done2:
	case <-time.After(time.Second):
		t.Fatal("seq=2 dispatch did not finish")
	}
}

// TestReorderDuplicate verifies old/duplicate frames are dropped.
func TestReorderDuplicate(t *testing.T) {
	m := NewManager(func(f protocol.Frame) error { return nil })
	m.Reorder = true

	var got []uint32
	deliver := func(f protocol.Frame) { got = append(got, f.SeqID) }

	m.DispatchFrame(protocol.Frame{StreamID: 1, SeqID: 1, Type: protocol.MsgData}, deliver)
	m.DispatchFrame(protocol.Frame{StreamID: 1, SeqID: 2, Type: protocol.MsgData}, deliver)
	// Duplicate / old:
	m.DispatchFrame(protocol.Frame{StreamID: 1, SeqID: 1, Type: protocol.MsgData}, deliver)
	m.DispatchFrame(protocol.Frame{StreamID: 1, SeqID: 3, Type: protocol.MsgData}, deliver)

	want := []uint32{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("len: got %d want %d (got=%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("position %d: got %d want %d", i, got[i], want[i])
		}
	}
}

func TestReorderGapLimitResetsStream(t *testing.T) {
	m := NewManager(func(f protocol.Frame) error { return nil })
	m.Reorder = true
	m.MaxReorderPending = 2
	m.MaxReorderDelay = time.Hour

	var got []protocol.Frame
	deliver := func(f protocol.Frame) { got = append(got, f) }

	// Missing SeqID=1; the third out-of-order frame exceeds the pending cap.
	m.DispatchFrame(protocol.Frame{StreamID: 1, SeqID: 2, Type: protocol.MsgData}, deliver)
	m.DispatchFrame(protocol.Frame{StreamID: 1, SeqID: 3, Type: protocol.MsgData}, deliver)
	m.DispatchFrame(protocol.Frame{StreamID: 1, SeqID: 4, Type: protocol.MsgData}, deliver)

	if len(got) != 1 {
		t.Fatalf("got %d delivered frames want 1 reset frame", len(got))
	}
	if got[0].Type != protocol.MsgRst || got[0].StreamID != 1 {
		t.Fatalf("got %+v want RST for stream 1", got[0])
	}
}

func TestRstBypassesReorderGap(t *testing.T) {
	m := NewManager(func(f protocol.Frame) error { return nil })
	m.Reorder = true
	m.MaxReorderDelay = time.Hour

	var got []protocol.Frame
	deliver := func(f protocol.Frame) { got = append(got, f) }

	m.DispatchFrame(protocol.Frame{StreamID: 1, SeqID: 2, Type: protocol.MsgData}, deliver)
	m.DispatchFrame(protocol.Frame{StreamID: 1, SeqID: 3, Type: protocol.MsgRst}, deliver)

	if len(got) != 1 {
		t.Fatalf("got %d delivered frames want immediate RST", len(got))
	}
	if got[0].Type != protocol.MsgRst || got[0].StreamID != 1 {
		t.Fatalf("got %+v want RST for stream 1", got[0])
	}
}

// TestSendFrameAssignsSeq verifies SeqID is assigned automatically on
// the send side.
func TestSendFrameAssignsSeq(t *testing.T) {
	var seqs []uint32
	m := NewManager(func(f protocol.Frame) error {
		seqs = append(seqs, f.SeqID)
		return nil
	})

	for i := 0; i < 5; i++ {
		_ = m.SendFrame(protocol.Frame{Type: protocol.MsgData, StreamID: 9})
	}

	want := []uint32{1, 2, 3, 4, 5}
	for i := range want {
		if seqs[i] != want[i] {
			t.Errorf("position %d: got %d want %d", i, seqs[i], want[i])
		}
	}

	// Different stream gets its own counter.
	seqs = nil
	_ = m.SendFrame(protocol.Frame{Type: protocol.MsgData, StreamID: 10})
	if len(seqs) != 1 || seqs[0] != 1 {
		t.Errorf("new stream: got %v want [1]", seqs)
	}
}

func TestSendFrameLeavesRstUnsequenced(t *testing.T) {
	var got protocol.Frame
	m := NewManager(func(f protocol.Frame) error {
		got = f
		return nil
	})

	if err := m.SendFrame(protocol.Frame{Type: protocol.MsgRst, StreamID: 9}); err != nil {
		t.Fatal(err)
	}
	if got.SeqID != 0 {
		t.Fatalf("RST SeqID = %d, want 0", got.SeqID)
	}
}

func TestReadLoopSendsRstOnDataSendFailure(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()

	frames := make(chan protocol.Frame, 4)
	m := NewManager(func(f protocol.Frame) error {
		frames <- f
		if f.Type == protocol.MsgData {
			return errors.New("append failed")
		}
		return nil
	})
	s := &Stream{ID: 1, Conn: a}

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.ReadLoop(s)
	}()

	if _, err := b.Write([]byte("upload")); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ReadLoop did not exit after DATA send failure")
	}

	var got []protocol.Frame
	for {
		select {
		case f := <-frames:
			got = append(got, f)
		default:
			if len(got) != 2 {
				t.Fatalf("got %d frames want DATA,RST: %+v", len(got), got)
			}
			if got[0].Type != protocol.MsgData {
				t.Fatalf("first frame = %s, want DATA", protocol.TypeName(got[0].Type))
			}
			if got[1].Type != protocol.MsgRst {
				t.Fatalf("second frame = %s, want RST", protocol.TypeName(got[1].Type))
			}
			if got[1].SeqID != 0 {
				t.Fatalf("RST SeqID = %d, want 0", got[1].SeqID)
			}
			return
		}
	}
}

// blockingConn is a net.Conn whose Write blocks until release() is
// called. Used to simulate a TCP peer that has stopped draining its
// receive buffer.
type blockingConn struct {
	net.Conn
	release chan struct{}
	wrote   chan struct{}
}

func newBlockingConn() (*blockingConn, net.Conn) {
	a, b := net.Pipe()
	bc := &blockingConn{
		Conn:    a,
		release: make(chan struct{}),
		wrote:   make(chan struct{}, 16),
	}
	// Drain `b` (the "consumer" side) just enough to unblock the very
	// first Write, then stop. After that, all subsequent Writes on `a`
	// block waiting on the kernel.
	go func() {
		buf := make([]byte, 64)
		_, _ = b.Read(buf) // unblock first write
		<-bc.release
		_ = b.Close()
	}()
	return bc, b
}

func (bc *blockingConn) Write(p []byte) (int, error) {
	n, err := bc.Conn.Write(p)
	select {
	case bc.wrote <- struct{}{}:
	default:
	}
	return n, err
}

// TestHandleDataResetsSustainedSlowConsumer proves that HandleData
// resets a stream whose TCP consumer stays blocked past the bounded
// backpressure window, without stalling unrelated streams.
func TestHandleDataResetsSustainedSlowConsumer(t *testing.T) {
	var sentMu sync.Mutex
	var sent []protocol.Frame
	m := NewManager(func(f protocol.Frame) error {
		sentMu.Lock()
		sent = append(sent, f)
		sentMu.Unlock()
		return nil
	})
	m.InboundQueueSize = 2 // small so it fills fast
	m.OutboundQueueWait = 5 * time.Millisecond

	// Slow stream: its conn is blocking on Write.
	slowConn, slowPeer := newBlockingConn()
	defer slowPeer.Close()
	slow := &Stream{ID: 1, Conn: slowConn}
	m.Register(slow)

	// Fast stream: conn drains immediately.
	fastConn, fastPeer := net.Pipe()
	defer fastConn.Close()
	defer fastPeer.Close()
	fast := &Stream{ID: 2, Conn: fastConn}
	m.Register(fast)

	// A background reader on the fast peer so writes don't block.
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := fastPeer.Read(buf); err != nil {
				return
			}
		}
	}()

	// Push enough frames into the slow stream to exhaust its queue
	// (size 2) and trigger an RST after the short test wait.
	deadline := time.Now().Add(2 * time.Second)
	for i := 0; i < 50 && time.Now().Before(deadline); i++ {
		done := make(chan struct{})
		go func() {
			m.HandleData(1, []byte("xxxxxxxxxx"))
			close(done)
		}()
		select {
		case <-done:
			// good — bounded wait completed
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("HandleData blocked on slow stream at iter %d — HOL!", i)
		}
		// give the writer goroutine a tick
		time.Sleep(1 * time.Millisecond)
	}

	// Now hammer the fast stream — every HandleData must also return
	// quickly even though the slow stream is still wedged.
	for i := 0; i < 20; i++ {
		done := make(chan struct{})
		go func() {
			m.HandleData(2, []byte("fast"))
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("HandleData blocked on fast stream at iter %d — slow stream HOL!", i)
		}
	}

	// Cleanup — release the slow conn so its writer can exit.
	close(slowConn.release)

	deadline = time.Now().Add(time.Second)
	for {
		sentMu.Lock()
		gotReset := len(sent) > 0 && sent[0].Type == protocol.MsgRst && sent[0].StreamID == 1
		sentMu.Unlock()
		if gotReset {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for slow-stream RST")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestOutboundQueueWaitsForTransientBackpressure(t *testing.T) {
	var sent []protocol.Frame
	m := NewManager(func(f protocol.Frame) error {
		sent = append(sent, f)
		return nil
	})
	m.OutboundQueueWait = 200 * time.Millisecond

	out := make(chan outboundEvent, 1)
	closeCh := make(chan struct{})
	s := &Stream{ID: 1, closeCh: closeCh}
	out <- outboundEvent{kind: protocol.MsgData, payload: []byte("queued")}

	done := make(chan struct{})
	go func() {
		m.enqueueOutboundOrReset(s, 1, out, closeCh, outboundEvent{kind: protocol.MsgData, payload: []byte("next")})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Millisecond):
		t.Fatalf("enqueue blocked the caller instead of deferring overflow")
	}

	first := <-out
	if got := string(first.payload); got != "queued" {
		t.Fatalf("first queued payload = %q, want queued", got)
	}

	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		select {
		case second := <-out:
			if got := string(second.payload); got != "next" {
				t.Fatalf("second queued payload = %q, want next", got)
			}
			if len(sent) != 0 {
				t.Fatalf("sent %d reset frames during transient queue pressure", len(sent))
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("overflow did not recover after transient queue pressure")
		}
		time.Sleep(time.Millisecond)
	}
}
