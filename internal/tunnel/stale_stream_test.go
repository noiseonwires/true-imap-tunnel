package tunnel

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/noiseonwires/true-imap-tunnel/internal/config"
	"github.com/noiseonwires/true-imap-tunnel/internal/protocol"
	"github.com/noiseonwires/true-imap-tunnel/internal/streams"
)

func TestRegisterAndSendOpenHandlesImmediateReplyData(t *testing.T) {
	streamID := protocol.MakeStreamID(7, 42)
	banner := []byte("SSH-2.0-test-server\r\n")
	tun := &Tunnel{
		cfg:          &config.Config{Mode: config.ModeClient},
		pendingOpens: make(map[uint32]chan protocol.Frame),
		pendingDials: make(map[uint32]*pendingDial),
	}

	var sent []protocol.Frame
	tun.streams = streams.NewManager(func(f protocol.Frame) error {
		sent = append(sent, f)
		if f.Type == protocol.MsgOpen {
			// Model one watcher fetch containing the OPEN_OK and the target's
			// immediate SSH banner. Both are dispatched before SendFrame returns.
			tun.dispatchOrdered(protocol.Frame{
				Type:     protocol.MsgOpenOK,
				StreamID: streamID,
				SeqID:    1,
			}, "primary")
			tun.dispatchOrdered(protocol.Frame{
				Type:     protocol.MsgData,
				StreamID: streamID,
				SeqID:    2,
				Payload:  banner,
			}, "primary")
		}
		return nil
	})

	responses := make(chan protocol.Frame, 1)
	tun.pendingOpens[streamID] = responses
	local, peer := net.Pipe()
	defer peer.Close()

	s := &streams.Stream{ID: streamID, Conn: local}
	if err := tun.registerAndSendOpen(s); err != nil {
		t.Fatalf("register and send OPEN: %v", err)
	}
	defer tun.streams.CloseStream(s)

	select {
	case resp := <-responses:
		if resp.Type != protocol.MsgOpenOK {
			t.Fatalf("response type = %s, want OPEN_OK", protocol.TypeName(resp.Type))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OPEN_OK")
	}

	got := make([]byte, len(banner))
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(peer, got); err != nil {
		t.Fatalf("read immediate DATA: %v", err)
	}
	if string(got) != string(banner) {
		t.Fatalf("immediate DATA = %q, want %q", got, banner)
	}

	for _, f := range sent {
		if f.Type == protocol.MsgRst {
			t.Fatalf("sent unexpected RST for immediate DATA: %+v", f)
		}
	}
}

func TestDispatchUnknownStreamDataOrFinSendsRst(t *testing.T) {
	for _, typ := range []byte{protocol.MsgData, protocol.MsgFin} {
		t.Run(protocol.TypeName(typ), func(t *testing.T) {
			tun, sent := newDispatchTestTunnel()

			streamID := protocol.MakeStreamID(7, 42)
			tun.dispatchOrdered(protocol.Frame{
				Type:     typ,
				StreamID: streamID,
				SeqID:    3,
				Payload:  []byte("stale"),
			}, "")

			if len(*sent) != 1 {
				t.Fatalf("sent %d frames, want 1 RST: %+v", len(*sent), *sent)
			}
			rst := (*sent)[0]
			if rst.Type != protocol.MsgRst || rst.StreamID != streamID {
				t.Fatalf("sent %+v, want RST for stream %d", rst, streamID)
			}
			if rst.SeqID != 0 {
				t.Fatalf("RST SeqID = %d, want 0", rst.SeqID)
			}
		})
	}
}

func TestHandleIncomingNeverSeenDataWaitsForOpen(t *testing.T) {
	tun, sent := newDispatchTestTunnel()
	tun.streams.Reorder = true

	streamID := protocol.MakeStreamID(7, 42)
	tun.handleIncomingFrame(protocol.Frame{
		Type:     protocol.MsgData,
		StreamID: streamID,
		SeqID:    153,
		Payload:  []byte("late"),
	}, "primary")

	if len(*sent) != 0 {
		t.Fatalf("sent %+v, want no RST because OPEN may still arrive", *sent)
	}
}

func TestHandleIncomingRecentlyClosedHighSeqDataWaitsForReusedOpen(t *testing.T) {
	tun, sent := newDispatchTestTunnel()
	tun.streams.Reorder = true

	streamID := protocol.MakeStreamID(7, 42)
	conn, peer := net.Pipe()
	tun.streams.Register(&streams.Stream{ID: streamID, Conn: conn})
	for seq := uint32(1); seq < 70; seq++ {
		tun.streams.DispatchFrame(protocol.Frame{
			Type:     protocol.MsgData,
			StreamID: streamID,
			SeqID:    seq,
		}, func(protocol.Frame) {})
	}
	tun.streams.CloseStream(tun.streams.Get(streamID))
	_ = peer.Close()

	tun.handleIncomingFrame(protocol.Frame{
		Type:     protocol.MsgData,
		StreamID: streamID,
		SeqID:    153,
		Payload:  []byte("late"),
	}, "primary")

	if len(*sent) != 0 {
		t.Fatalf("sent %+v, want no RST because a reused stream's OPEN may still arrive", *sent)
	}
}

func TestHandleIncomingRecentlyClosedLowSeqWaitsForReusedOpen(t *testing.T) {
	tun, sent := newDispatchTestTunnel()
	tun.streams.Reorder = true

	streamID := protocol.MakeStreamID(7, 42)
	conn, peer := net.Pipe()
	tun.streams.Register(&streams.Stream{ID: streamID, Conn: conn})
	for seq := uint32(1); seq < 70; seq++ {
		tun.streams.DispatchFrame(protocol.Frame{
			Type:     protocol.MsgData,
			StreamID: streamID,
			SeqID:    seq,
		}, func(protocol.Frame) {})
	}
	tun.streams.CloseStream(tun.streams.Get(streamID))
	_ = peer.Close()

	tun.handleIncomingFrame(protocol.Frame{
		Type:     protocol.MsgData,
		StreamID: streamID,
		SeqID:    2,
		Payload:  []byte("new early data"),
	}, "primary")

	if len(*sent) != 0 {
		t.Fatalf("sent %+v, want no RST because a reused stream's OPEN may still arrive", *sent)
	}
}

func TestDispatchPendingDialDataDoesNotSendUnknownRst(t *testing.T) {
	tun, sent := newDispatchTestTunnel()
	streamID := protocol.MakeStreamID(7, 42)
	pd := &pendingDial{done: make(chan struct{})}
	tun.pendingDials[streamID] = pd

	tun.dispatchOrdered(protocol.Frame{
		Type:     protocol.MsgData,
		StreamID: streamID,
		SeqID:    2,
		Payload:  []byte("early"),
	}, "")

	if len(*sent) != 0 {
		t.Fatalf("sent %+v, want no RST while pending dial buffers DATA", *sent)
	}
	if len(pd.buffered) != 1 || pd.buffered[0].kind != protocol.MsgData {
		t.Fatalf("pending buffer = %+v, want one DATA event", pd.buffered)
	}
}

func TestDispatchOpenRejectedDuringShutdown(t *testing.T) {
	tun, sent := newDispatchTestTunnel()
	tun.shuttingDown.Store(true)

	streamID := protocol.MakeStreamID(3, 9)
	tun.dispatchOrdered(protocol.Frame{Type: protocol.MsgOpen, StreamID: streamID}, "acct")

	if len(*sent) != 1 {
		t.Fatalf("sent %d frames, want 1 OPEN_FAIL: %+v", len(*sent), *sent)
	}
	if f := (*sent)[0]; f.Type != protocol.MsgOpenFail || f.StreamID != streamID {
		t.Fatalf("sent %+v, want OPEN_FAIL for stream %d", f, streamID)
	}
	tun.pendingDialsMu.Lock()
	n := len(tun.pendingDials)
	tun.pendingDialsMu.Unlock()
	if n != 0 {
		t.Fatalf("pendingDials = %d, want 0 (no dial started during shutdown)", n)
	}
}

func newDispatchTestTunnel() (*Tunnel, *[]protocol.Frame) {
	var sent []protocol.Frame
	tun := &Tunnel{
		cfg:          &config.Config{Mode: config.ModeServer},
		pendingOpens: make(map[uint32]chan protocol.Frame),
		pendingDials: make(map[uint32]*pendingDial),
	}
	tun.streams = streams.NewManager(func(f protocol.Frame) error {
		sent = append(sent, f)
		return nil
	})
	return tun, &sent
}
