package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestBatchRoundTripSingle(t *testing.T) {
	f := Frame{Type: MsgOpen, StreamID: 7, SeqID: 1, Payload: []byte("x")}
	blob, err := EncodeBatch([]Frame{f})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	if blob[0] != BatchMagic {
		t.Errorf("missing magic, got 0x%02x", blob[0])
	}
	out, err := DecodeBatch(blob)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("got %d frames want 1", len(out))
	}
	if out[0].Type != f.Type || out[0].StreamID != f.StreamID ||
		out[0].SeqID != f.SeqID || !bytes.Equal(out[0].Payload, f.Payload) {
		t.Errorf("round-trip mismatch: got %+v want %+v", out[0], f)
	}
}

func TestBatchRoundTripMany(t *testing.T) {
	in := []Frame{
		{Type: MsgOpen, StreamID: 1, SeqID: 1},
		{Type: MsgData, StreamID: 1, SeqID: 2, Payload: []byte("hello")},
		{Type: MsgData, StreamID: 2, SeqID: 1, Payload: bytes.Repeat([]byte{0x42}, 1024)},
		{Type: MsgFin, StreamID: 1, SeqID: 3},
		{Type: MsgRst, StreamID: 2, SeqID: 2},
	}
	blob, err := EncodeBatch(in)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	out, err := DecodeBatch(blob)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("got %d frames want %d", len(out), len(in))
	}
	for i := range in {
		if out[i].Type != in[i].Type || out[i].StreamID != in[i].StreamID ||
			out[i].SeqID != in[i].SeqID || !bytes.Equal(out[i].Payload, in[i].Payload) {
			t.Errorf("[%d] round-trip mismatch", i)
		}
	}
}

func TestBatchEmpty(t *testing.T) {
	if _, err := EncodeBatch(nil); err == nil {
		t.Errorf("expected error on empty batch")
	}
	if _, err := DecodeBatch(nil); err == nil {
		t.Errorf("expected error on empty blob")
	}
}

func TestBatchBadMagic(t *testing.T) {
	if _, err := DecodeBatch([]byte{0x00, 0x00, 0x01}); err == nil {
		t.Errorf("expected magic error")
	}
}

func TestBatchTruncated(t *testing.T) {
	blob, _ := EncodeBatch([]Frame{{Type: MsgData, StreamID: 1, SeqID: 1, Payload: []byte("hi")}})
	for cut := len(blob) - 1; cut >= 1; cut-- {
		if _, err := DecodeBatch(blob[:cut]); err == nil {
			t.Errorf("expected truncation error at cut=%d", cut)
		}
	}
}

func TestBatchTrailingGarbage(t *testing.T) {
	blob, _ := EncodeBatch([]Frame{{Type: MsgData, StreamID: 1, SeqID: 1}})
	blob = append(blob, 0xFF, 0xFF)
	if _, err := DecodeBatch(blob); err == nil {
		t.Errorf("expected trailing-garbage error")
	}
}

func TestIsBatch(t *testing.T) {
	if !IsBatch([]byte{BatchMagic, 0, 0}) {
		t.Errorf("IsBatch should detect magic")
	}
	if IsBatch([]byte{MsgData}) {
		t.Errorf("IsBatch should reject plain frame")
	}
	if IsBatch(nil) {
		t.Errorf("IsBatch(nil)")
	}
}

func BenchmarkEncodeBatch(b *testing.B) {
	for _, size := range []int{64, 32768} {
		for _, count := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("bytes=%d/frames=%d", size, count), func(b *testing.B) {
				frames := make([]Frame, count)
				for index := range frames {
					frames[index] = Frame{Type: MsgData, StreamID: 1, SeqID: uint32(index + 1), Payload: bytes.Repeat([]byte{0x42}, size)}
				}
				b.SetBytes(int64(size * count))
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if _, err := EncodeBatch(frames); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestEncodeBatchWireCompatibility(t *testing.T) {
	for _, size := range []int{0, 1, 64, 32768} {
		frames := []Frame{{Type: MsgOpen, StreamID: 0x07010203, SeqID: 1}, {Type: MsgData, StreamID: 0x07010203, SeqID: 2, Payload: bytes.Repeat([]byte{0x42}, size)}}
		var expected bytes.Buffer
		expected.Write([]byte{0xBA, 0, 2})
		for _, frame := range frames {
			_ = binary.Write(&expected, binary.BigEndian, uint32(9+len(frame.Payload)))
			expected.WriteByte(frame.Type)
			_ = binary.Write(&expected, binary.BigEndian, frame.StreamID)
			_ = binary.Write(&expected, binary.BigEndian, frame.SeqID)
			expected.Write(frame.Payload)
		}
		actual, err := EncodeBatch(frames)
		if err != nil || !bytes.Equal(actual, expected.Bytes()) {
			t.Fatalf("legacy wire mismatch for %d bytes: %v", size, err)
		}
	}
}
