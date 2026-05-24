package protocol

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		f    Frame
	}{
		{"empty data", Frame{Type: MsgData, StreamID: 7, SeqID: 42, Payload: nil}},
		{"open no payload", Frame{Type: MsgOpen, StreamID: 1, SeqID: 1}},
		{"open_fail with reason", Frame{Type: MsgOpenFail, StreamID: 9, SeqID: 1, Payload: []byte("dial: timeout")}},
		{"large data", Frame{Type: MsgData, StreamID: 3, SeqID: 99, Payload: bytes.Repeat([]byte{0xAA}, 4096)}},
		{"max ids", Frame{Type: MsgData, StreamID: 0xFFFFFFFF, SeqID: 0xFFFFFFFF, Payload: []byte("x")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := Encode(c.f)
			if len(b) < HeaderSize {
				t.Fatalf("encoded too short: %d", len(b))
			}
			got, err := Decode(b)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got.Type != c.f.Type {
				t.Errorf("type: got %#x want %#x", got.Type, c.f.Type)
			}
			if got.StreamID != c.f.StreamID {
				t.Errorf("streamID: got %d want %d", got.StreamID, c.f.StreamID)
			}
			if got.SeqID != c.f.SeqID {
				t.Errorf("seqID: got %d want %d", got.SeqID, c.f.SeqID)
			}
			if !bytes.Equal(got.Payload, c.f.Payload) {
				t.Errorf("payload mismatch")
			}
		})
	}
}

func TestDecodeShort(t *testing.T) {
	if _, err := Decode([]byte{0x10, 0, 0, 0}); err == nil {
		t.Fatal("expected error on short input")
	}
	if _, err := Decode(nil); err == nil {
		t.Fatal("expected error on nil input")
	}
}

func TestTypeName(t *testing.T) {
	want := map[byte]string{
		MsgOpen: "OPEN", MsgOpenOK: "OPEN_OK", MsgOpenFail: "OPEN_FAIL",
		MsgData: "DATA", MsgFin: "FIN", MsgRst: "RST", MsgPing: "PING",
	}
	for t1, name := range want {
		if got := TypeName(t1); got != name {
			t.Errorf("TypeName(%#x) = %s, want %s", t1, got, name)
		}
	}
	if got := TypeName(0x99); got != "0x99" {
		t.Errorf("TypeName(0x99) = %s, want 0x99", got)
	}
}

func TestIsOrdered(t *testing.T) {
	ordered := []byte{MsgOpen, MsgOpenOK, MsgOpenFail, MsgData, MsgFin}
	for _, typ := range ordered {
		if !IsOrdered(typ) {
			t.Errorf("IsOrdered(%s) = false, want true", TypeName(typ))
		}
	}

	unordered := []byte{MsgRst, MsgPing, MsgPong, 0x99}
	for _, typ := range unordered {
		if IsOrdered(typ) {
			t.Errorf("IsOrdered(%s) = true, want false", TypeName(typ))
		}
	}
}
