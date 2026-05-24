package netprotect

import (
	"net"
	"sync"
	"syscall"
	"time"
)

type ControlFunc func(network, address string, c syscall.RawConn) error

var (
	mu      sync.RWMutex
	control ControlFunc
)

// Install sets a process-wide socket control hook used by outbound dials.
func Install(fn ControlFunc) {
	mu.Lock()
	control = fn
	mu.Unlock()
}

// WrapDialer returns a copy of d with the process-wide socket control hook
// chained after any existing dialer Control callback.
func WrapDialer(d *net.Dialer) *net.Dialer {
	if d == nil {
		d = &net.Dialer{Timeout: 30 * time.Second}
	}
	wrapped := *d
	fn := current()
	if fn == nil {
		return &wrapped
	}
	existing := wrapped.Control
	wrapped.Control = func(network, address string, c syscall.RawConn) error {
		if existing != nil {
			if err := existing(network, address, c); err != nil {
				return err
			}
		}
		return fn(network, address, c)
	}
	return &wrapped
}

func current() ControlFunc {
	mu.RLock()
	defer mu.RUnlock()
	return control
}
