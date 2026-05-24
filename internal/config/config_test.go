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
