// Package tlog is a minimal level-aware logging facade. It wraps the
// standard log package, prefixing each line with a level tag and
// filtering by a global threshold.
//
// Levels (most → least severe):
//
//	ERROR  — failures the operator should look at
//	WARN   — recoverable problems (reconnects, dropped frames)
//	INFO   — high-level lifecycle (default)
//	DEBUG  — useful for diagnostics; per-stream events
//	TRACE  — extremely chatty; per-frame TX/RX
//
// The threshold is global and is set once at startup via SetLevel. There
// is no per-component override; if more granularity is needed later,
// add a per-call category check before formatting.
package tlog

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
)

// Level orders messages from most to least severe.
type Level int32

const (
	LevelError Level = iota
	LevelWarn
	LevelInfo
	LevelDebug
	LevelTrace
)

var level atomic.Int32

func init() { level.Store(int32(LevelInfo)) }

// SetLevel installs the global threshold.
func SetLevel(l Level) { level.Store(int32(l)) }

// CurrentLevel returns the current threshold.
func CurrentLevel() Level { return Level(level.Load()) }

// Enabled reports whether messages at level l would be emitted.
// Hot paths can call this to skip costly fmt.Sprintf work.
func Enabled(l Level) bool { return l <= Level(level.Load()) }

// ParseLevel maps a config-friendly string ("info", "debug", …) to a
// Level. Unknown values fall back to LevelInfo and return false.
func ParseLevel(s string) (Level, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return LevelInfo, true
	case "error", "err":
		return LevelError, true
	case "warn", "warning":
		return LevelWarn, true
	case "debug":
		return LevelDebug, true
	case "trace", "verbose":
		return LevelTrace, true
	}
	return LevelInfo, false
}

// String returns the canonical lowercase name of a Level.
func (l Level) String() string {
	switch l {
	case LevelError:
		return "error"
	case LevelWarn:
		return "warn"
	case LevelInfo:
		return "info"
	case LevelDebug:
		return "debug"
	case LevelTrace:
		return "trace"
	}
	return fmt.Sprintf("level(%d)", int32(l))
}

func emit(tag string, format string, args ...any) {
	// Add the level tag in the prefix slot used by the original log
	// lines. The standard logger prepends date/time itself.
	log.Output(3, "["+tag+"] "+fmt.Sprintf(format, args...))
}

// Errorf logs at LevelError.
func Errorf(format string, args ...any) {
	if Enabled(LevelError) {
		emit("ERROR", format, args...)
	}
}

// Warnf logs at LevelWarn.
func Warnf(format string, args ...any) {
	if Enabled(LevelWarn) {
		emit("WARN", format, args...)
	}
}

// Infof logs at LevelInfo.
func Infof(format string, args ...any) {
	if Enabled(LevelInfo) {
		emit("INFO", format, args...)
	}
}

// Debugf logs at LevelDebug.
func Debugf(format string, args ...any) {
	if Enabled(LevelDebug) {
		emit("DEBUG", format, args...)
	}
}

// Tracef logs at LevelTrace.
func Tracef(format string, args ...any) {
	if Enabled(LevelTrace) {
		emit("TRACE", format, args...)
	}
}
