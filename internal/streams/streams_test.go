package streams

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/noiseonwires/true-imap-tunnel/internal/protocol"
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

func TestReorderGapResetNotifiesPeerAndDropsLateFrames(t *testing.T) {
	m := NewManager(func(f protocol.Frame) error { return nil })
	m.Reorder = true
	m.MaxReorderPending = 2
	m.MaxReorderDelay = time.Hour

	var resetIDs []uint32
	m.OnReorderReset = func(streamID uint32) {
		resetIDs = append(resetIDs, streamID)
	}

	var got []protocol.Frame
	deliver := func(f protocol.Frame) { got = append(got, f) }

	m.DispatchFrame(protocol.Frame{StreamID: 7, SeqID: 2, Type: protocol.MsgData}, deliver)
	m.DispatchFrame(protocol.Frame{StreamID: 7, SeqID: 3, Type: protocol.MsgData}, deliver)
	m.DispatchFrame(protocol.Frame{StreamID: 7, SeqID: 4, Type: protocol.MsgData}, deliver)

	for seq := uint32(5); seq < 20; seq++ {
		m.DispatchFrame(protocol.Frame{StreamID: 7, SeqID: seq, Type: protocol.MsgData}, deliver)
	}

	if len(resetIDs) != 1 || resetIDs[0] != 7 {
		t.Fatalf("reset callbacks = %v, want [7]", resetIDs)
	}
	if len(got) != 1 || got[0].Type != protocol.MsgRst || got[0].StreamID != 7 {
		t.Fatalf("delivered %+v, want one local RST", got)
	}
}

func TestReorderTombstoneRejectsReplayedOpen(t *testing.T) {
	m := NewManager(func(f protocol.Frame) error { return nil })
	m.Reorder = true

	m.Remove(9)

	var got []protocol.Frame
	deliver := func(f protocol.Frame) { got = append(got, f) }

	m.DispatchFrame(protocol.Frame{StreamID: 9, SeqID: 2, Type: protocol.MsgData}, deliver)
	m.DispatchFrame(protocol.Frame{StreamID: 9, SeqID: 1, Type: protocol.MsgOpen}, deliver)

	if len(got) != 0 {
		t.Fatalf("replayed closed stream delivered: %+v", got)
	}
}

func TestDuplicateOpenPreservesReceiveSequence(t *testing.T) {
	for _, pending := range []bool{false, true} {
		manager := NewManager(func(protocol.Frame) error { return nil })
		manager.Reorder = true
		var delivered []uint32
		handler := func(frame protocol.Frame) { delivered = append(delivered, frame.SeqID) }
		open := protocol.Frame{Type: protocol.MsgOpen, StreamID: 1, SeqID: 1}
		second := protocol.Frame{Type: protocol.MsgData, StreamID: 1, SeqID: 2}
		third := protocol.Frame{Type: protocol.MsgData, StreamID: 1, SeqID: 3}
		manager.DispatchFrame(open, handler)
		if pending {
			manager.DispatchFrame(third, handler)
		} else {
			manager.DispatchFrame(second, handler)
		}
		manager.DispatchFrame(open, handler)
		manager.DispatchFrame(second, handler)
		manager.DispatchFrame(third, handler)
		if len(delivered) != 3 || delivered[0] != 1 || delivered[1] != 2 || delivered[2] != 3 {
			t.Fatalf("pending=%v delivered=%v, want [1 2 3]", pending, delivered)
		}
	}
}

func TestStreamIDAllocationSkipsReservedIDs(t *testing.T) {
	manager := NewManager(func(protocol.Frame) error { return nil })
	manager.nextID.Store(protocol.StreamLocalIDMask)
	first := manager.NextIDForClient(7)
	manager.Remove(first)
	manager.nextID.Store(protocol.StreamLocalIDMask)
	second := manager.NextIDForClient(7)
	if second != protocol.MakeStreamID(7, 1) {
		t.Fatalf("wrapped allocation=%d", second)
	}
	manager.nextID.Store(1)
	if next := manager.NextIDForClient(7); next != protocol.MakeStreamID(7, 2) {
		t.Fatalf("reserved allocation=%d", next)
	}
}

func TestQuietReorderGapDoesNotIntroduceReset(t *testing.T) {
	manager := NewManager(func(protocol.Frame) error { return nil })
	manager.Reorder = true
	var delivered []uint32
	deliver := func(frame protocol.Frame) { delivered = append(delivered, frame.SeqID) }
	manager.DispatchFrame(protocol.Frame{Type: protocol.MsgData, StreamID: 1, SeqID: 1}, deliver)
	manager.DispatchFrame(protocol.Frame{Type: protocol.MsgData, StreamID: 1, SeqID: 3, Payload: []byte("third")}, deliver)
	future := time.Now().Add(time.Hour)
	gap := manager.ReorderGaps(future)
	if len(gap) != 1 || gap[0].AgeMs < 3500000 || gap[0].PendingBytes != 5 || gap[0].ExpectedSeq != 2 {
		t.Fatalf("gaps=%+v", gap)
	}
	manager.DispatchFrame(protocol.Frame{Type: protocol.MsgData, StreamID: 1, SeqID: 3, Payload: []byte("third")}, deliver)
	if again := manager.ReorderGaps(future); len(again) != 1 || again[0] != gap[0] {
		t.Fatalf("duplicate changed gap: %+v", again)
	}
	manager.DispatchFrame(protocol.Frame{Type: protocol.MsgData, StreamID: 1, SeqID: 2}, deliver)
	if len(delivered) != 3 || delivered[1] != 2 || delivered[2] != 3 || len(manager.ReorderGaps(future)) != 0 {
		t.Fatalf("delivery=%v gaps=%+v", delivered, manager.ReorderGaps(future))
	}
	manager.DispatchFrame(protocol.Frame{Type: protocol.MsgData, StreamID: 2, SeqID: 5, Payload: []byte("orphan")}, deliver)
	if len(manager.ReorderGaps(future)) != 1 {
		t.Fatal("orphan observation evicted state")
	}
	manager.CloseAll()
	if len(manager.ReorderGaps(future)) != 0 {
		t.Fatal("shutdown retained gap")
	}
}

func TestReorderDiagnosticCloseRace(t *testing.T) {
	manager := NewManager(func(protocol.Frame) error { return nil })
	manager.Reorder = true
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		for count := 0; count < 1000; count++ {
			manager.ReorderGaps(time.Now())
		}
	}()
	for id := uint32(1); id < 100; id++ {
		manager.DispatchFrame(protocol.Frame{Type: protocol.MsgData, StreamID: id, SeqID: 3}, func(protocol.Frame) {})
		manager.Remove(id)
	}
	workers.Wait()
	if len(manager.ReorderGaps(time.Now())) != 0 {
		t.Fatal("closed streams in diagnostics")
	}
}

func TestSimultaneousFinRemovesStreamOnce(t *testing.T) {
	manager := NewManager(func(protocol.Frame) error { return nil })
	local, remote := net.Pipe()
	defer remote.Close()
	stream := &Stream{ID: 1, Conn: local}
	manager.Register(stream)
	removed := make(chan uint32, 2)
	manager.OnRemove = func(id uint32) { removed <- id }
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); manager.finishRead(stream) }()
	go func() { defer workers.Done(); manager.FinishWrite(stream) }()
	workers.Wait()
	if len(removed) != 1 || manager.Count() != 0 {
		t.Fatal("FIN did not remove exactly once")
	}
	<-stream.writerDone
}

func TestStreamAbortSendsResetOnceAndNeverEchoesPeerReset(t *testing.T) {
	for _, peerReset := range []bool{false, true} {
		resets := make(chan uint32, 4)
		manager := NewManager(func(protocol.Frame) error { return nil })
		manager.OnLocalReset = func(id uint32) { resets <- id }
		stream := &Stream{ID: 1, Conn: &eofTestConn{}}
		manager.Register(stream)
		if peerReset {
			manager.HandleRst(1)
		}
		var workers sync.WaitGroup
		workers.Add(2)
		go func() { defer workers.Done(); manager.abortStream(stream) }()
		go func() { defer workers.Done(); manager.abortStream(stream) }()
		workers.Wait()
		want := 1
		if peerReset {
			want = 0
		}
		if len(resets) != want {
			t.Fatalf("peerReset=%v resets=%d want=%d", peerReset, len(resets), want)
		}
		<-stream.writerDone
	}
}

func TestFinSendFailureAbortsStream(t *testing.T) {
	manager := NewManager(func(frame protocol.Frame) error {
		if frame.Type == protocol.MsgFin {
			return errors.New("append failed")
		}
		return nil
	})
	stream := &Stream{ID: 1, Conn: &eofTestConn{}}
	manager.Register(stream)
	manager.ReadLoop(stream)
	if manager.Count() != 0 {
		t.Fatal("failed FIN retained stream")
	}
	<-stream.writerDone
}

type eofTestConn struct{ net.Conn }

func (*eofTestConn) Read([]byte) (int, error) { return 0, io.EOF }
func (*eofTestConn) Close() error             { return nil }

func TestRemoteFinWaitsForQueuedData(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	peer, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	local, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewManager(func(protocol.Frame) error { return nil })
	stream := &Stream{ID: 1, Conn: local}
	manager.Register(stream)
	defer manager.CloseAll()
	manager.HandleData(1, []byte("first"))
	manager.HandleData(1, []byte("second"))
	manager.HandleFin(1)
	_ = peer.SetDeadline(time.Now().Add(time.Second))
	body, err := io.ReadAll(peer)
	if err != nil || string(body) != "firstsecond" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	if manager.Count() != 1 {
		t.Fatal("remote FIN closed both directions")
	}
	_, _ = peer.Write([]byte("still readable"))
	_ = local.SetReadDeadline(time.Now().Add(time.Second))
	buffer := make([]byte, 14)
	if _, err := io.ReadFull(local, buffer); err != nil || string(buffer) != "still readable" {
		t.Fatalf("opposite direction: %q %v", buffer, err)
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
