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
	"sync"
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

// KeyRing selects a frame encryption key by stream client ID. It preserves the
// historical single-key behavior through defaultKey while allowing servers to
// use per-client keys for non-zero client IDs.
type KeyRing struct {
	defaultKey *AEAD
	clients    map[byte]*AEAD
	recentMu   sync.Mutex
	recent     []byte
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

// NewKeyRing builds a key ring from the default passphrase and optional
// per-client passphrases. Empty passphrases disable that key.
func NewKeyRing(defaultPassphrase string, clientPassphrases map[byte]string) (*KeyRing, error) {
	defaultKey, err := New(defaultPassphrase)
	if err != nil {
		return nil, err
	}
	clients := make(map[byte]*AEAD, len(clientPassphrases))
	for id, passphrase := range clientPassphrases {
		key, err := New(passphrase)
		if err != nil {
			return nil, fmt.Errorf("client %d: %w", id, err)
		}
		if key != nil {
			clients[id] = key
		}
	}
	if defaultKey == nil && len(clients) == 0 {
		return nil, nil
	}
	return &KeyRing{defaultKey: defaultKey, clients: clients}, nil
}

// Enabled reports whether encryption is configured. A nil receiver
// returns false.
func (a *AEAD) Enabled() bool { return a != nil }

// Enabled reports whether any encryption key is configured.
func (r *KeyRing) Enabled() bool {
	return r != nil && (r.defaultKey != nil || len(r.clients) > 0)
}

// ClientKeys reports the number of configured per-client keys.
func (r *KeyRing) ClientKeys() int {
	if r == nil {
		return 0
	}
	return len(r.clients)
}

// Overhead returns the per-frame ciphertext expansion (nonce + auth tag).
// Useful for size accounting; returns 0 when disabled.
func (a *AEAD) Overhead() int {
	if a == nil {
		return 0
	}
	return a.aead.NonceSize() + a.aead.Overhead()
}

// Overhead returns the per-frame ciphertext expansion for enabled keys.
func (r *KeyRing) Overhead() int {
	if r == nil {
		return 0
	}
	if r.defaultKey != nil {
		return r.defaultKey.Overhead()
	}
	for _, key := range r.clients {
		return key.Overhead()
	}
	return 0
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

// Encrypt seals plaintext with the per-client key when clientID is non-zero
// and configured. Otherwise it uses the default key. If per-client keys are
// configured and no key exists for a non-zero clientID, encryption fails unless
// a default key is available for legacy fallback.
func (r *KeyRing) Encrypt(plaintext []byte, clientID byte) ([]byte, error) {
	if r == nil {
		return plaintext, nil
	}
	if clientID != 0 {
		if key := r.clients[clientID]; key != nil {
			return key.Encrypt(plaintext)
		}
		if len(r.clients) > 0 && r.defaultKey == nil {
			return nil, fmt.Errorf("missing encryption key for client_id %d", clientID)
		}
	}
	return r.defaultKey.Encrypt(plaintext)
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

// Decrypt opens blob. If hintClientID names a configured per-client key, that
// key is tried first. The returned keyClientID is non-zero only when a
// per-client key succeeded; callers can verify decoded frames match it.
func (r *KeyRing) Decrypt(blob []byte, hintClientID byte) ([]byte, byte, error) {
	if r == nil {
		return blob, 0, nil
	}
	if hintClientID != 0 {
		if key := r.clients[hintClientID]; key != nil {
			pt, err := key.Decrypt(blob)
			if err == nil {
				r.promoteRecent(hintClientID)
				return pt, hintClientID, nil
			}
			if r.defaultKey == nil {
				return nil, 0, err
			}
		}
	}
	if r.defaultKey != nil {
		pt, err := r.defaultKey.Decrypt(blob)
		if err == nil {
			return pt, 0, nil
		}
	}
	recent := r.recentClientIDs()
	for _, id := range recent {
		if id == hintClientID {
			continue
		}
		key := r.clients[id]
		if key == nil {
			continue
		}
		pt, err := key.Decrypt(blob)
		if err == nil {
			r.promoteRecent(id)
			return pt, id, nil
		}
	}
	for id, key := range r.clients {
		if id == hintClientID || containsClientID(recent, id) {
			continue
		}
		pt, err := key.Decrypt(blob)
		if err == nil {
			r.promoteRecent(id)
			return pt, id, nil
		}
	}
	return nil, 0, errors.New("no configured encryption key could decrypt frame")
}

func (r *KeyRing) promoteRecent(clientID byte) {
	if r == nil || clientID == 0 {
		return
	}
	r.recentMu.Lock()
	defer r.recentMu.Unlock()
	for i, id := range r.recent {
		if id == clientID {
			copy(r.recent[1:i+1], r.recent[:i])
			r.recent[0] = clientID
			return
		}
	}
	r.recent = append(r.recent, 0)
	copy(r.recent[1:], r.recent[:len(r.recent)-1])
	r.recent[0] = clientID
}

func (r *KeyRing) recentClientIDs() []byte {
	if r == nil {
		return nil
	}
	r.recentMu.Lock()
	defer r.recentMu.Unlock()
	out := make([]byte, len(r.recent))
	copy(out, r.recent)
	return out
}

func containsClientID(ids []byte, clientID byte) bool {
	for _, id := range ids {
		if id == clientID {
			return true
		}
	}
	return false
}
