package tunnel

import (
	"net"
	"testing"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/config"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/protocol"
	"github.com/true-imap-tunnel/true-imap-tunnel/internal/streams"
)

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
