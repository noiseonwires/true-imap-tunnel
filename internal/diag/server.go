package diag

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/tlog"
)

const (
	DefaultAndroidAddr = "127.123.45.67:17680"
	defaultLogLimit    = 300
	maxLogLineBytes    = 2048
)

type SnapshotFunc func() any

type LogEntry struct {
	Time    string `json:"time"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

type logRing struct {
	mu      sync.Mutex
	entries []LogEntry
	next    int
	full    bool
}

type Server struct {
	addr     string
	started  time.Time
	snapshot SnapshotFunc
	logs     *logRing
	server   *http.Server
}

func NewServer(addr string, snapshot SnapshotFunc) *Server {
	return &Server{
		addr:     strings.TrimSpace(addr),
		started:  time.Now(),
		snapshot: snapshot,
		logs:     newLogRing(defaultLogLimit),
	}
}

func Disabled(addr string) bool {
	switch strings.ToLower(strings.TrimSpace(addr)) {
	case "", "0", "off", "false", "disabled", "none":
		return true
	default:
		return false
	}
}

func newLogRing(limit int) *logRing {
	if limit <= 0 {
		limit = defaultLogLimit
	}
	return &logRing{entries: make([]LogEntry, 0, limit)}
}

func (r *logRing) add(e LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) < cap(r.entries) {
		r.entries = append(r.entries, e)
		return
	}
	r.entries[r.next] = e
	r.next = (r.next + 1) % len(r.entries)
	r.full = true
}

func (r *logRing) snapshot() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.full {
		return append([]LogEntry(nil), r.entries...)
	}
	out := make([]LogEntry, 0, len(r.entries))
	out = append(out, r.entries[r.next:]...)
	out = append(out, r.entries[:r.next]...)
	return out
}

func (s *Server) InstallLogSink() {
	tlog.AddSink(func(at time.Time, level tlog.Level, line string) {
		if len(line) > maxLogLineBytes {
			line = line[:maxLogLineBytes] + "...(truncated)"
		}
		s.logs.add(LogEntry{
			Time:    at.Format(time.RFC3339Nano),
			Level:   level.String(),
			Message: line,
		})
	})
}

func (s *Server) Start(ctx context.Context) error {
	if Disabled(s.addr) {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/logs", s.handleLogs)

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.server.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := s.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			tlog.Warnf("status API stopped: %v", err)
		}
	}()
	tlog.Infof("status API listening on http://%s", s.addr)
	return nil
}

func (s *Server) handleRoot(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("true-imap-tunnel status API\n/status\n/logs\n"))
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	payload := map[string]any{
		"daemon":         "true-imap-tunnel",
		"generated_at":   time.Now().Format(time.RFC3339Nano),
		"uptime_seconds": int64(time.Since(s.started).Seconds()),
	}
	if s.snapshot != nil {
		payload["tunnel"] = s.snapshot()
	}
	writeJSON(w, payload)
}

func (s *Server) handleLogs(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"entries": s.logs.snapshot(),
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
