package crypto

import (
	"bytes"
	"strings"
	"testing"
)

func TestDisabledRoundTrip(t *testing.T) {
	a, err := New("")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a != nil {
		t.Errorf("New(empty) should return nil AEAD, got %v", a)
	}
	if a.Enabled() {
		t.Errorf("nil AEAD should report disabled")
	}
	in := []byte("hello world")
	out, err := a.Encrypt(in)
	if err != nil {
		t.Fatalf("Encrypt on nil: %v", err)
	}
	if !bytes.Equal(in, out) {
		t.Errorf("nil Encrypt should be a no-op")
	}
	back, err := a.Decrypt(out)
	if err != nil {
		t.Fatalf("Decrypt on nil: %v", err)
	}
	if !bytes.Equal(in, back) {
		t.Errorf("nil Decrypt should be a no-op")
	}
}

func TestRoundTrip(t *testing.T) {
	a, err := New("hunter2")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !a.Enabled() {
		t.Errorf("expected enabled")
	}
	if a.Overhead() != 12+16 {
		t.Errorf("overhead: got %d want %d", a.Overhead(), 12+16)
	}
	cases := [][]byte{
		nil,
		{},
		[]byte("x"),
		[]byte("hello, tunnel"),
		bytes.Repeat([]byte{0xAA}, 65536),
	}
	for i, in := range cases {
		blob, err := a.Encrypt(in)
		if err != nil {
			t.Fatalf("[%d] Encrypt: %v", i, err)
		}
		if bytes.Equal(blob, in) && len(in) > 0 {
			t.Errorf("[%d] ciphertext equals plaintext", i)
		}
		back, err := a.Decrypt(blob)
		if err != nil {
			t.Fatalf("[%d] Decrypt: %v", i, err)
		}
		if !bytes.Equal(back, in) {
			t.Errorf("[%d] round-trip mismatch", i)
		}
	}
}

func TestDifferentNoncesPerCall(t *testing.T) {
	a, _ := New("k")
	in := []byte("same plaintext")
	b1, _ := a.Encrypt(in)
	b2, _ := a.Encrypt(in)
	if bytes.Equal(b1, b2) {
		t.Errorf("same input produced same ciphertext — nonce not random")
	}
}

func TestWrongKeyFails(t *testing.T) {
	a, _ := New("right-key")
	b, _ := New("wrong-key")
	blob, _ := a.Encrypt([]byte("secret"))
	if _, err := b.Decrypt(blob); err == nil {
		t.Errorf("decrypt with wrong key should fail")
	}
}

func TestTamperingFails(t *testing.T) {
	a, _ := New("k")
	blob, _ := a.Encrypt([]byte("secret"))
	// Flip a bit in the ciphertext (skip past the nonce).
	blob[len(blob)-1] ^= 0x01
	if _, err := a.Decrypt(blob); err == nil {
		t.Errorf("decrypt of tampered ciphertext should fail")
	}
}

func TestTooShortFails(t *testing.T) {
	a, _ := New("k")
	if _, err := a.Decrypt([]byte{1, 2, 3}); err == nil || !strings.Contains(err.Error(), "too short") {
		t.Errorf("expected too-short error, got %v", err)
	}
}
