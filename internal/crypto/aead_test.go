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

func TestKeyRingUsesPerClientKeys(t *testing.T) {
	ring, err := NewKeyRing("", map[byte]string{
		7: "client-seven",
		8: "client-eight",
	})
	if err != nil {
		t.Fatalf("NewKeyRing: %v", err)
	}

	blob, err := ring.Encrypt([]byte("secret"), 7)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := ring.clients[8].Decrypt(blob); err == nil {
		t.Fatal("client 8 key decrypted client 7 ciphertext")
	}
	plain, keyID, err := ring.Decrypt(blob, 7)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if keyID != 7 || string(plain) != "secret" {
		t.Fatalf("Decrypt = %q with key %d, want secret with key 7", plain, keyID)
	}
}

func TestKeyRingPromotesRecentlyDecryptedClientKeys(t *testing.T) {
	ring, err := NewKeyRing("", map[byte]string{
		7: "client-seven",
		8: "client-eight",
	})
	if err != nil {
		t.Fatalf("NewKeyRing: %v", err)
	}

	blob7, err := ring.Encrypt([]byte("seven"), 7)
	if err != nil {
		t.Fatalf("Encrypt client 7: %v", err)
	}
	if _, keyID, err := ring.Decrypt(blob7, 7); err != nil || keyID != 7 {
		t.Fatalf("Decrypt hinted client 7 keyID=%d err=%v", keyID, err)
	}
	if got := ring.recentClientIDs(); !bytes.Equal(got, []byte{7}) {
		t.Fatalf("recent after client 7 = %v, want [7]", got)
	}

	blob8, err := ring.Encrypt([]byte("eight"), 8)
	if err != nil {
		t.Fatalf("Encrypt client 8: %v", err)
	}
	if _, keyID, err := ring.Decrypt(blob8, 0); err != nil || keyID != 8 {
		t.Fatalf("Decrypt unhinted client 8 keyID=%d err=%v", keyID, err)
	}
	if got := ring.recentClientIDs(); !bytes.Equal(got, []byte{8, 7}) {
		t.Fatalf("recent after client 8 = %v, want [8 7]", got)
	}

	if _, keyID, err := ring.Decrypt(blob7, 0); err != nil || keyID != 7 {
		t.Fatalf("Decrypt unhinted client 7 keyID=%d err=%v", keyID, err)
	}
	if got := ring.recentClientIDs(); !bytes.Equal(got, []byte{7, 8}) {
		t.Fatalf("recent after client 7 again = %v, want [7 8]", got)
	}
}

func TestKeyRingMissingClientKeyFailsWithoutDefault(t *testing.T) {
	ring, err := NewKeyRing("", map[byte]string{7: "client-seven"})
	if err != nil {
		t.Fatalf("NewKeyRing: %v", err)
	}
	if _, err := ring.Encrypt([]byte("secret"), 8); err == nil {
		t.Fatal("Encrypt accepted missing client key without default fallback")
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
