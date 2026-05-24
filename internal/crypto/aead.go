// Package crypto provides a tiny symmetric-encryption wrapper around
// AES-256-GCM, used to obscure the contents of frames carried inside
// IMAP messages from a curious email provider.
//
// The goal is *content concealment*, not strong security: the IMAP
// transport itself is already protected by TLS, so an attacker would
// need either the IMAP credentials or a passive position on the
// IMAP server to even see the ciphertext. What this package adds is
// a layer that prevents the IMAP server operator (who has full
// read-access to the stored messages) from inspecting our wire
// protocol or its payloads.
//
// Algorithm choices:
//
//   - AES-256-GCM: stdlib (crypto/aes + crypto/cipher), authenticated.
//     Hardware-accelerated on every CPU we care about. The 16-byte
//     auth tag also doubles as a "did we use the right key" check —
//     mismatched configs fail loudly with a Decrypt error rather
//     than silently garbling frames.
//
//   - SHA-256 key derivation: passphrase → 32-byte key. Not a strong
//     KDF (no salt, no work factor) — but the threat model here is
//     "casual inspection by the IMAP provider", not "offline brute
//     force by a nation state". For stronger key handling, supply a
//     32-byte random key out-of-band and pre-hash it yourself.
//
//   - Per-frame random 96-bit nonce, prepended to the ciphertext.
//     GCM requires unique-per-key nonces; random 96 bits is safe up
//     to ~2^32 frames per key, more than any realistic tunnel will
//     ever ship.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
)

// AEAD is a frame-level encrypt/decrypt helper.
//
// A nil *AEAD is valid and means "encryption disabled" — every method
// short-circuits to pass-through. This lets callers write
//
//	out, err := a.Encrypt(in)
//
// without first checking whether encryption is configured.
type AEAD struct {
	aead cipher.AEAD
}

// New derives a key from the passphrase (SHA-256) and returns an
// AEAD. An empty passphrase returns (nil, nil) — encryption disabled.
func New(passphrase string) (*AEAD, error) {
	if passphrase == "" {
		return nil, nil
	}
	sum := sha256.Sum256([]byte(passphrase))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, fmt.Errorf("aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm: %w", err)
	}
	return &AEAD{aead: gcm}, nil
}

// Enabled reports whether encryption is configured. A nil receiver
// returns false.
func (a *AEAD) Enabled() bool { return a != nil }

// Overhead returns the per-frame ciphertext expansion (nonce + auth tag).
// Useful for size accounting; returns 0 when disabled.
func (a *AEAD) Overhead() int {
	if a == nil {
		return 0
	}
	return a.aead.NonceSize() + a.aead.Overhead()
}

// Encrypt seals plaintext with a fresh random nonce. The returned
// blob is laid out as: nonce || ciphertext || auth-tag. When the
// receiver is nil (encryption disabled) the plaintext is returned
// unchanged.
func (a *AEAD) Encrypt(plaintext []byte) ([]byte, error) {
	if a == nil {
		return plaintext, nil
	}
	ns := a.aead.NonceSize()
	nonce := make([]byte, ns)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("rand: %w", err)
	}
	// Pre-allocate the full buffer to avoid a second copy.
	dst := make([]byte, 0, ns+len(plaintext)+a.aead.Overhead())
	dst = append(dst, nonce...)
	return a.aead.Seal(dst, nonce, plaintext, nil), nil
}

// Decrypt parses nonce || ciphertext+tag and returns the plaintext.
// Returns an error when the auth tag doesn't verify, which covers
// both tampering and wrong-key (mismatched-config) cases.
//
// When the receiver is nil, blob is returned unchanged.
func (a *AEAD) Decrypt(blob []byte) ([]byte, error) {
	if a == nil {
		return blob, nil
	}
	ns := a.aead.NonceSize()
	if len(blob) < ns+a.aead.Overhead() {
		return nil, errors.New("ciphertext too short")
	}
	nonce, ct := blob[:ns], blob[ns:]
	pt, err := a.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("aead open: %w", err)
	}
	return pt, nil
}
