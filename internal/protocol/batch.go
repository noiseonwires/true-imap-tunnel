package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// BatchMagic is the first byte of every batch-encoded IMAP message
// body (after optional decryption). It distinguishes the batched
// envelope from a raw single-frame encoding (which would otherwise
// start with a frame type byte in the range 0x10..0xF1).
//
// Multi-frame IMAP messages use this batch envelope. Single-frame
// messages may be sent as raw Encode(Frame) bytes to avoid envelope
// overhead; decoders distinguish the formats by this leading byte.
const BatchMagic byte = 0xBA

// Batch wire format:
//
//	[1B  BatchMagic = 0xBA]
//	[2B  count, big-endian]
//	repeated count times:
//	  [4B frame length, big-endian]
//	  [frame bytes — exactly that length]
//
// Frame bytes inside the batch are the same as Encode(Frame): a
// 9-byte header followed by the payload.

// EncodeBatch packs one or more frames into a single batch envelope.
// Returns an error if more than 65535 frames are supplied (a 2-byte
// count won't fit them).
func EncodeBatch(frames []Frame) ([]byte, error) {
	if len(frames) == 0 {
		return nil, errors.New("empty batch")
	}
	if len(frames) > 0xFFFF {
		return nil, fmt.Errorf("batch too large: %d frames > 65535", len(frames))
	}

	// Pre-compute size.
	size := 1 + 2 // magic + count
	for i := range frames {
		size += 4 + HeaderSize + len(frames[i].Payload)
	}

	out := make([]byte, 0, size)
	out = append(out, BatchMagic)
	out = binary.BigEndian.AppendUint16(out, uint16(len(frames)))
	for i := range frames {
		enc := Encode(frames[i])
		out = binary.BigEndian.AppendUint32(out, uint32(len(enc)))
		out = append(out, enc...)
	}
	return out, nil
}

// DecodeBatch parses a batch envelope and returns each contained
// frame. Payloads alias the input slice; callers that retain frames
// past the input buffer's lifetime must copy.
func DecodeBatch(blob []byte) ([]Frame, error) {
	if len(blob) < 3 {
		return nil, errors.New("batch too short")
	}
	if blob[0] != BatchMagic {
		return nil, fmt.Errorf("not a batch: leading byte = 0x%02x, want 0x%02x",
			blob[0], BatchMagic)
	}
	count := int(binary.BigEndian.Uint16(blob[1:3]))
	if count == 0 {
		return nil, errors.New("batch declares zero frames")
	}
	pos := 3
	out := make([]Frame, 0, count)
	for i := 0; i < count; i++ {
		if pos+4 > len(blob) {
			return nil, fmt.Errorf("batch truncated at frame %d/%d (length header)", i+1, count)
		}
		frameLen := int(binary.BigEndian.Uint32(blob[pos : pos+4]))
		pos += 4
		if frameLen < HeaderSize {
			return nil, fmt.Errorf("batch frame %d/%d too small: %d bytes",
				i+1, count, frameLen)
		}
		if pos+frameLen > len(blob) {
			return nil, fmt.Errorf("batch truncated at frame %d/%d (body, want %d bytes, have %d)",
				i+1, count, frameLen, len(blob)-pos)
		}
		f, err := Decode(blob[pos : pos+frameLen])
		if err != nil {
			return nil, fmt.Errorf("batch frame %d/%d decode: %w", i+1, count, err)
		}
		out = append(out, f)
		pos += frameLen
	}
	if pos != len(blob) {
		return nil, fmt.Errorf("batch trailing garbage: %d extra bytes", len(blob)-pos)
	}
	return out, nil
}

// IsBatch reports whether blob starts with the batch magic byte.
// Useful for callers that want to gracefully handle either raw single
// frames or multi-frame batch envelopes.
func IsBatch(blob []byte) bool {
	return len(blob) > 0 && blob[0] == BatchMagic
}
