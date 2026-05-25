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
