package config

import (
	"testing"
	"time"
)

const batchDelayConfigBase = `
mode: client
listen: "127.0.0.1:1080"
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    folder_send: "c2s"
    folder_recv: "s2c"
`

func TestBatchDelayDefaultIsTwoMilliseconds(t *testing.T) {
	cfg, err := LoadBytes([]byte(batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.BatchDelay(); got != 2*time.Millisecond {
		t.Fatalf("BatchDelay() = %v, want 2ms", got)
	}
}

func TestBatchDelayExplicitZeroDisables(t *testing.T) {
	cfg, err := LoadBytes([]byte("batch_delay_ms: 0\n" + batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.BatchDelay(); got != 0 {
		t.Fatalf("BatchDelay() = %v, want 0", got)
	}
}

func TestFetchUIDOverlapDefaultAndDisable(t *testing.T) {
	cfg, err := LoadBytes([]byte(batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.FetchUIDOverlap(); got != 0 {
		t.Fatalf("FetchUIDOverlap() = %d, want 0", got)
	}

	cfg, err = LoadBytes([]byte("fetch_uid_overlap: 0\n" + batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.FetchUIDOverlap(); got != 0 {
		t.Fatalf("FetchUIDOverlap() = %d, want 0", got)
	}
}

func TestStartupCleanupConnectionDefault(t *testing.T) {
	cfg, err := LoadBytes([]byte(batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.EffectiveStartupCleanupConnection(); got != StartupCleanupConnectionFallback {
		t.Fatalf("EffectiveStartupCleanupConnection() = %q, want %q",
			got, StartupCleanupConnectionFallback)
	}
}

func TestStartupCleanupConnectionDedicated(t *testing.T) {
	cfg, err := LoadBytes([]byte("startup_cleanup_connection: dedicated\n" + batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.EffectiveStartupCleanupConnection(); got != StartupCleanupConnectionDedicated {
		t.Fatalf("EffectiveStartupCleanupConnection() = %q, want %q",
			got, StartupCleanupConnectionDedicated)
	}
}

func TestStartupCleanupConnectionMain(t *testing.T) {
	cfg, err := LoadBytes([]byte("startup_cleanup_connection: main\n" + batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.EffectiveStartupCleanupConnection(); got != StartupCleanupConnectionMain {
		t.Fatalf("EffectiveStartupCleanupConnection() = %q, want %q",
			got, StartupCleanupConnectionMain)
	}
}

func TestStartupCleanupConnectionInvalid(t *testing.T) {
	if _, err := LoadBytes([]byte("startup_cleanup_connection: shared\n" + batchDelayConfigBase)); err == nil {
		t.Fatal("LoadBytes accepted invalid startup_cleanup_connection")
	}
}

func TestMessageSubjectDefault(t *testing.T) {
	cfg, err := LoadBytes([]byte(batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.EffectiveMessageSubject(); got != DefaultMessageSubject {
		t.Fatalf("EffectiveMessageSubject() = %q, want %q", got, DefaultMessageSubject)
	}
	if got := cfg.EffectiveMessageSubjectMode(); got != MessageSubjectModeFixed {
		t.Fatalf("EffectiveMessageSubjectMode() = %q, want %q", got, MessageSubjectModeFixed)
	}
	if !cfg.SubjectClientIDEnabled() {
		t.Fatal("SubjectClientIDEnabled() = false, want default true")
	}
	if got := cfg.Accounts[0].EffectiveMessageFrom(); got != "u@example.com" {
		t.Fatalf("EffectiveMessageFrom() = %q, want derived sender", got)
	}
	if got := cfg.EffectiveMessageTo(); got != DefaultMessageTo {
		t.Fatalf("EffectiveMessageTo() = %q, want %q", got, DefaultMessageTo)
	}
}

func TestMessageSubjectCustom(t *testing.T) {
	cfg, err := LoadBytes([]byte("message_subject: \"Quarterly update\"\n" + batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.EffectiveMessageSubject(); got != "Quarterly update" {
		t.Fatalf("EffectiveMessageSubject() = %q, want custom subject", got)
	}
}

func TestMessageSubjectRandomMode(t *testing.T) {
	cfg, err := LoadBytes([]byte("message_subject_mode: random\n" + batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.EffectiveMessageSubjectMode(); got != MessageSubjectModeRandom {
		t.Fatalf("EffectiveMessageSubjectMode() = %q, want %q", got, MessageSubjectModeRandom)
	}
}

func TestMessageSubjectInvalidMode(t *testing.T) {
	if _, err := LoadBytes([]byte("message_subject_mode: rotating\n" + batchDelayConfigBase)); err == nil {
		t.Fatal("LoadBytes accepted invalid message_subject_mode")
	}
}

func TestMessageSubjectRejectsCRLF(t *testing.T) {
	if _, err := LoadBytes([]byte("message_subject: \"ok\\r\\nInjected: yes\"\n" + batchDelayConfigBase)); err == nil {
		t.Fatal("LoadBytes accepted message_subject containing CRLF")
	}
}

func TestSubjectClientIDDisabled(t *testing.T) {
	cfg, err := LoadBytes([]byte("subject_client_id: false\n" + batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SubjectClientIDEnabled() {
		t.Fatal("SubjectClientIDEnabled() = true, want false")
	}
}

func TestMessageFromCustomPerAccount(t *testing.T) {
	cfg, err := LoadBytes([]byte("" + `
mode: client
listen: "127.0.0.1:1080"
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    folder_send: "c2s"
    folder_recv: "s2c"
    message_from: "sender@example.com"
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Accounts[0].EffectiveMessageFrom(); got != "sender@example.com" {
		t.Fatalf("EffectiveMessageFrom() = %q, want custom sender", got)
	}
}

func TestMessageFromRejectsCRLF(t *testing.T) {
	if _, err := LoadBytes([]byte("" + `
mode: client
listen: "127.0.0.1:1080"
accounts:
  - host: "imap.example.com:993"
    username: "u"
    password: "p"
    folder_send: "c2s"
    folder_recv: "s2c"
    message_from: "ok\r\nInjected: yes"
`)); err == nil {
		t.Fatal("LoadBytes accepted message_from containing CRLF")
	}
}

func TestMessageToCustomFixed(t *testing.T) {
	cfg, err := LoadBytes([]byte("message_to: \"receiver@example.com\"\n" + batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.EffectiveMessageTo(); got != "receiver@example.com" {
		t.Fatalf("EffectiveMessageTo() = %q, want custom receiver", got)
	}
}

func TestMessageToTemplate(t *testing.T) {
	cfg, err := LoadBytes([]byte("message_to: \"receiver+{random}@example.com\"\n" + batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.EffectiveMessageTo(); got != "receiver+{random}@example.com" {
		t.Fatalf("EffectiveMessageTo() = %q, want template", got)
	}
}

func TestMessageToRejectsCRLF(t *testing.T) {
	if _, err := LoadBytes([]byte("message_to: \"ok\r\nInjected: yes\"\n" + batchDelayConfigBase)); err == nil {
		t.Fatal("LoadBytes accepted message_to containing CRLF")
	}
}

func TestClientEncryptionPassphrases(t *testing.T) {
	cfg, err := LoadBytes([]byte("client_encryption_passphrases:\n  7: seven-secret\n  8: eight-secret\n" + batchDelayConfigBase))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientEncryptionPassphrases[7] != "seven-secret" || cfg.ClientEncryptionPassphrases[8] != "eight-secret" {
		t.Fatalf("client encryption passphrases = %#v", cfg.ClientEncryptionPassphrases)
	}
}

func TestClientEncryptionPassphrasesRejectZero(t *testing.T) {
	if _, err := LoadBytes([]byte("client_encryption_passphrases:\n  0: zero-secret\n" + batchDelayConfigBase)); err == nil {
		t.Fatal("LoadBytes accepted client_encryption_passphrases key 0")
	}
}
