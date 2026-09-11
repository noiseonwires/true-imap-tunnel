package tunnel

import (
	"context"
	"time"

	"github.com/noiseonwires/true-imap-tunnel/internal/protocol"
	"github.com/noiseonwires/true-imap-tunnel/internal/tlog"
)

type openResult struct {
	response protocol.Frame
	expires  time.Time
	closed   bool
}

type controlReply struct {
	frame protocol.Frame
	label string
	done  chan struct{}
}

func (t *Tunnel) flushControls() {
	done := make(chan struct{})
	t.controlMu.Lock()
	if t.controlStopped {
		t.controlMu.Unlock()
		return
	}
	t.controls = append(t.controls, controlReply{done: done})
	t.controlMu.Unlock()
	select {
	case t.controlWake <- struct{}{}:
	default:
	}
	select {
	case <-done:
	case <-t.baseCtx.Done():
	}
}

func (t *Tunnel) retireOpen(streamID uint32) {
	t.pendingDialsMu.Lock()
	if result := t.openResults[streamID]; result != nil {
		result.closed = true
		result.expires = time.Now().Add(t.streams.Retention())
	}
	t.pendingDialsMu.Unlock()
}

func (t *Tunnel) replayOpen(streamID uint32) {
	t.pendingDialsMu.Lock()
	result := t.openResults[streamID]
	frame := protocol.Frame{Type: protocol.MsgRst, StreamID: streamID}
	if result != nil && !result.closed {
		frame = result.response
	}
	t.pendingDialsMu.Unlock()
	if frame.Type != 0 {
		t.queueControl(frame, "")
	}
}

func (t *Tunnel) sendOpenResponse(streamID uint32, kind byte, payload []byte) error {
	frame := protocol.Frame{Type: kind, StreamID: streamID, SeqID: 1, Payload: payload}
	t.pendingDialsMu.Lock()
	if result := t.openResults[streamID]; result != nil {
		result.response = frame
		if kind == protocol.MsgOpenFail {
			result.expires = time.Now().Add(t.streams.Retention())
		}
	}
	t.pendingDialsMu.Unlock()
	return t.streams.SendFrame(frame)
}

func (t *Tunnel) sweepOpenResults(now time.Time) {
	t.pendingDialsMu.Lock()
	defer t.pendingDialsMu.Unlock()
	for id, result := range t.openResults {
		if !result.expires.IsZero() && !now.Before(result.expires) && t.pendingDials[id] == nil {
			t.streams.ForgetInactive(id)
			delete(t.openResults, id)
		}
	}
}

func (t *Tunnel) queueControl(frame protocol.Frame, label string) {
	t.controlMu.Lock()
	if !t.controlStopped {
		t.controls = append(t.controls, controlReply{frame: frame, label: label})
	}
	t.controlMu.Unlock()
	select {
	case t.controlWake <- struct{}{}:
	default:
	}
}

func (t *Tunnel) runControls(ctx context.Context) {
	defer close(t.controlDone)
	defer func() {
		t.controlMu.Lock()
		t.controlStopped = true
		t.controls = nil
		t.controlMu.Unlock()
	}()
	ticker := time.NewTicker(t.streams.Retention())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			t.sweepOpenResults(now)
			t.streams.SweepExpired(now)
		case <-t.controlWake:
		}
		for ctx.Err() == nil {
			t.controlMu.Lock()
			if len(t.controls) == 0 {
				t.controls = nil
				t.controlMu.Unlock()
				break
			}
			reply := t.controls[0]
			t.controls[0] = controlReply{}
			t.controls = t.controls[1:]
			t.controlMu.Unlock()
			if reply.done != nil {
				close(reply.done)
				continue
			}
			var err error
			if reply.label == "" {
				err = t.streams.SendFrame(reply.frame)
			} else {
				err = t.paths.SendViaLabel(reply.label, reply.frame)
			}
			if err != nil && ctx.Err() == nil {
				tlog.Debugf("control send failed: %v", err)
			}
		}
	}
}
