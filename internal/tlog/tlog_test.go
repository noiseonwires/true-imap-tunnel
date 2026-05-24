package tlog

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]Level{
		"":        LevelInfo,
		"info":    LevelInfo,
		"INFO":    LevelInfo,
		"warn":    LevelWarn,
		"warning": LevelWarn,
		"error":   LevelError,
		"err":     LevelError,
		"debug":   LevelDebug,
		"trace":   LevelTrace,
		"verbose": LevelTrace,
	}
	for s, want := range cases {
		got, ok := ParseLevel(s)
		if !ok {
			t.Errorf("ParseLevel(%q): not ok", s)
		}
		if got != want {
			t.Errorf("ParseLevel(%q): got %s want %s", s, got, want)
		}
	}
	if _, ok := ParseLevel("nonsense"); ok {
		t.Errorf("ParseLevel(nonsense): expected !ok")
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	saved := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(saved)
	savedFlags := log.Flags()
	log.SetFlags(0)
	defer log.SetFlags(savedFlags)

	prev := CurrentLevel()
	defer SetLevel(prev)

	SetLevel(LevelWarn)
	Errorf("e")
	Warnf("w")
	Infof("i")
	Debugf("d")
	Tracef("t")

	out := buf.String()
	if !strings.Contains(out, "[ERROR] e") {
		t.Errorf("missing ERROR line: %q", out)
	}
	if !strings.Contains(out, "[WARN] w") {
		t.Errorf("missing WARN line: %q", out)
	}
	if strings.Contains(out, "[INFO]") {
		t.Errorf("unexpected INFO line at LevelWarn: %q", out)
	}
	if strings.Contains(out, "[DEBUG]") {
		t.Errorf("unexpected DEBUG line: %q", out)
	}
	if strings.Contains(out, "[TRACE]") {
		t.Errorf("unexpected TRACE line: %q", out)
	}
}

func TestEnabled(t *testing.T) {
	prev := CurrentLevel()
	defer SetLevel(prev)

	SetLevel(LevelInfo)
	if !Enabled(LevelError) || !Enabled(LevelInfo) {
		t.Error("expected Error and Info enabled at LevelInfo")
	}
	if Enabled(LevelDebug) || Enabled(LevelTrace) {
		t.Error("did not expect Debug/Trace enabled at LevelInfo")
	}
}
