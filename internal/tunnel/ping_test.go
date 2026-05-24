package tunnel

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"
)

func TestBuildPingPayloadIncludesTimestampAndClientVersion(t *testing.T) {
	now := time.Unix(0, 123456789)
	payload := buildPingPayload(now, "  android/0.1.0  ")

	if len(payload) != 8+len("android/0.1.0") {
		t.Fatalf("payload len = %d", len(payload))
	}
	if got := int64(binary.BigEndian.Uint64(payload[:8])); got != now.UnixNano() {
		t.Fatalf("timestamp = %d", got)
	}
	if got := pingClientVersion(payload); got != "android/0.1.0" {
		t.Fatalf("client version = %q", got)
	}
}

func TestBuildPingPayloadKeepsLegacyTimestampOnlyWhenVersionEmpty(t *testing.T) {
	payload := buildPingPayload(time.Unix(0, 1), "  ")

	if len(payload) != 8 {
		t.Fatalf("payload len = %d", len(payload))
	}
	if got := pingClientVersion(payload); got != "" {
		t.Fatalf("client version = %q", got)
	}
}

func TestBuildPingPayloadCapsClientVersion(t *testing.T) {
	payload := buildPingPayload(time.Unix(0, 1), strings.Repeat("x", maxPingClientVersionLen+1))

	if got := len(payload) - 8; got != maxPingClientVersionLen {
		t.Fatalf("version len = %d", got)
	}
}
