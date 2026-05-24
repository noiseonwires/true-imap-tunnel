// Package protocol defines the wire format for frames carried over IMAP.
//
// Each frame is encoded as a contiguous byte sequence:
//
//	[1B type][4B streamID BE][4B seqID BE][payload...]
//
// One frame is carried per IMAP message: the frame bytes are base64-encoded
// and wrapped in a minimal RFC 5322 message that is APPENDed to a folder.
//
// Stream IDs are allocated by the client side (the side that listens for
// local TCP connections); the server side echoes the same ID in its
// OPEN_OK / OPEN_FAIL / DATA / FIN / RST frames. SeqIDs are assigned per
// stream per direction, starting at 1, by the sender; the receiver uses
// them to reorder frames (essential when multiple IMAP accounts are used
// in parallel, since each account is an independent transport path with
// its own latency).
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Frame types.
const (
	// MsgOpen is sent by the client when a new local TCP connection is
	// accepted. The receiver dials its configured target and replies with
	// MsgOpenOK on success or MsgOpenFail (payload = ASCII reason) on
	// failure.
	MsgOpen byte = 0x10

	// MsgOpenOK confirms a stream is established.
	MsgOpenOK byte = 0x11

	// MsgOpenFail reports that a stream could not be established. Payload
	// is a human-readable reason (UTF-8).
	MsgOpenFail byte = 0x12

	// MsgData carries TCP payload bytes.
	MsgData byte = 0x20

	// MsgFin signals a graceful half-close: the sender has finished
	// writing on this stream.
	MsgFin byte = 0x21

	// MsgRst aborts a stream immediately.
	MsgRst byte = 0x22

	// MsgPing is sent by either side to measure round-trip latency.
	// The first 8 payload bytes are the big-endian UnixNano send
	// timestamp of the sender. Optional bytes after that are diagnostic
	// metadata (currently a UTF-8 client version string). The recipient
	// must echo the whole payload back as MsgPong, unmodified. The
	// original sender then computes RTT = now - timestamp.
	//
	// Frames with type Ping/Pong use SeqID 0 and are not affected by the
	// reorder buffer. In multi-client mode, clients stamp their client ID
	// into StreamID so the echoed Pong can be filtered by recipient.
	MsgPing byte = 0xF0

	// MsgPong is the echoed response to MsgPing. See MsgPing for the
	// payload semantics.
	MsgPong byte = 0xF1
)

// Stream-ID layout (uint32, big-endian on the wire):
//
//	bits 31..24  clientID (1..255; 0 = single-client / unassigned)
//	bits 23..0   localID  (per-client counter, ~16 M values)
//
// In multi-client deployments — where several client instances share the
// same IMAP folder pair against one server — the top byte distinguishes
// streams that belong to different clients. The server preserves it in
// its responses, so each client can filter (by top byte) the messages it
// fetches and leave the others in place for their intended recipient.
const (
	StreamClientIDShift        = 24
	StreamLocalIDMask   uint32 = 0x00FFFFFF
)

// StreamClientID returns the client-ID byte stamped into a stream ID.
// Returns 0 for streams created in single-client mode.
func StreamClientID(streamID uint32) byte {
	return byte(streamID >> StreamClientIDShift)
}

// MakeStreamID combines a client ID and a 24-bit local ID into a stream ID.
// The localID is masked to 24 bits so callers can pass arbitrary uint32
// counter values.
func MakeStreamID(clientID byte, localID uint32) uint32 {
	return (uint32(clientID) << StreamClientIDShift) | (localID & StreamLocalIDMask)
}

// HeaderSize is the fixed-size header before the payload.
const HeaderSize = 1 + 4 + 4

// Frame is a single protocol message.
type Frame struct {
	Type     byte
	StreamID uint32
	SeqID    uint32
	Payload  []byte
}

// IsOrdered reports whether a frame type participates in per-stream
// SeqID ordering. OPEN/OPEN_OK/OPEN_FAIL/DATA/FIN are ordered; RST,
// PING/PONG, and any unknown types are not.
//
// The sender stamps SeqIDs onto ordered frames; the receiver routes
// them through the reorder buffer. Abort/control frames use SeqID=0 and
// bypass reordering so a stream can be torn down even if a previous DATA
// frame was lost.
func IsOrdered(t byte) bool {
	switch t {
	case MsgOpen, MsgOpenOK, MsgOpenFail, MsgData, MsgFin:
		return true
	default:
		return false
	}
}

// TypeName returns a short string for the frame type, useful in logs.
func TypeName(t byte) string {
	switch t {
	case MsgOpen:
		return "OPEN"
	case MsgOpenOK:
		return "OPEN_OK"
	case MsgOpenFail:
		return "OPEN_FAIL"
	case MsgData:
		return "DATA"
	case MsgFin:
		return "FIN"
	case MsgRst:
		return "RST"
	case MsgPing:
		return "PING"
	case MsgPong:
		return "PONG"
	default:
		return fmt.Sprintf("0x%02x", t)
	}
}

// Encode serialises a Frame to a byte slice.
func Encode(f Frame) []byte {
	buf := make([]byte, HeaderSize+len(f.Payload))
	buf[0] = f.Type
	binary.BigEndian.PutUint32(buf[1:5], f.StreamID)
	binary.BigEndian.PutUint32(buf[5:9], f.SeqID)
	copy(buf[HeaderSize:], f.Payload)
	return buf
}

// Decode parses a byte slice into a Frame. The returned Payload aliases the
// input slice; callers that need to retain it past the caller's buffer
// lifetime must copy.
func Decode(data []byte) (Frame, error) {
	if len(data) < HeaderSize {
		return Frame{}, errors.New("frame too short")
	}
	return Frame{
		Type:     data[0],
		StreamID: binary.BigEndian.Uint32(data[1:5]),
		SeqID:    binary.BigEndian.Uint32(data[5:9]),
		Payload:  data[HeaderSize:],
	}, nil
}
