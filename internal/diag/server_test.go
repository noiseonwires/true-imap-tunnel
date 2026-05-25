package diag

import "testing"

func TestLogRingKeepsMostRecentEntries(t *testing.T) {
	r := newLogRing(3)
	for _, msg := range []string{"one", "two", "three", "four", "five"} {
		r.add(LogEntry{Message: msg})
	}
	got := r.snapshot()
	want := []string{"three", "four", "five"}
	if len(got) != len(want) {
		t.Fatalf("snapshot len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Message != want[i] {
			t.Fatalf("snapshot[%d] = %q, want %q", i, got[i].Message, want[i])
		}
	}
}

func TestDisabledStatusAddr(t *testing.T) {
	for _, addr := range []string{"", "off", " false ", "disabled", "none", "0"} {
		if !Disabled(addr) {
			t.Fatalf("Disabled(%q) = false, want true", addr)
		}
	}
	if Disabled(DefaultAndroidAddr) {
		t.Fatalf("Disabled(%q) = true, want false", DefaultAndroidAddr)
	}
}
