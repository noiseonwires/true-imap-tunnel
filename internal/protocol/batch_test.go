package protocol

import (
	"bytes"
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
