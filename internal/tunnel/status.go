package tunnel

import "time"

type StatusSnapshot struct {
	Mode              string          `json:"mode"`
	Listen            string          `json:"listen,omitempty"`
	Target            string          `json:"target,omitempty"`
	ClientID          uint8           `json:"client_id,omitempty"`
	ClientVersion     string          `json:"client_version,omitempty"`
	ActiveStreams     int             `json:"active_streams"`
	AccountsTotal     int             `json:"accounts_total"`
	AccountsConnected int             `json:"accounts_connected"`
	AccountsProven    int             `json:"accounts_proven"`
	Accounts          []AccountStatus `json:"accounts"`
}

type AccountStatus struct {
	Label                   string `json:"label"`
	SenderConnected         bool   `json:"sender_connected"`
	SenderConnectedSeconds  int64  `json:"sender_connected_seconds,omitempty"`
	SenderConnectCount      uint64 `json:"sender_connect_count"`
	SentFrames              uint64 `json:"sent_frames"`
	AppendBatches           uint64 `json:"append_batches"`
	WatcherConnected        bool   `json:"watcher_connected"`
	WatcherConnectedSeconds int64  `json:"watcher_connected_seconds,omitempty"`
	WatcherConnectCount     uint64 `json:"watcher_connect_count"`
	ReceiveReady            bool   `json:"receive_ready"`
	IdleSupported           *bool  `json:"idle_supported,omitempty"`
	FramesReceived          uint64 `json:"frames_received"`
	LastFrameAt             string `json:"last_frame_at,omitempty"`
	LastFrameAgeMs          int64  `json:"last_frame_age_ms,omitempty"`
	Proven                  bool   `json:"proven"`
}

func (t *Tunnel) StatusSnapshot() StatusSnapshot {
	s := StatusSnapshot{
		Mode:              string(t.cfg.Mode),
		Listen:            t.cfg.Listen,
		Target:            t.cfg.Target,
		ClientID:          t.cfg.ClientID,
		ClientVersion:     t.cfg.ClientVersion,
		ActiveStreams:     t.streams.Count(),
		AccountsTotal:     t.paths.SenderCount(),
		AccountsConnected: t.paths.ConnectedCount(),
		AccountsProven:    t.paths.ProvenCount(),
	}
	s.Accounts = t.paths.AccountStatuses()
	return s
}

func (m *Multipath) AccountStatuses() []AccountStatus {
	out := make([]AccountStatus, 0, len(m.senders))
	now := time.Now()
	for i, sender := range m.senders {
		status := AccountStatus{
			Label:              sender.Label(),
			SenderConnected:    sender.Connected(),
			SenderConnectCount: sender.ConnectCount(),
			SentFrames:         sender.SentCount(),
			AppendBatches:      sender.BatchCount(),
			Proven:             m.proven(i),
		}
		if status.SenderConnected {
			if connectedAt := sender.ConnectedAt(); !connectedAt.IsZero() {
				status.SenderConnectedSeconds = int64(now.Sub(connectedAt).Seconds())
			}
		}
		if i < len(m.watchers) {
			watcher := m.watchers[i]
			status.WatcherConnected = watcher.Connected()
			status.WatcherConnectCount = watcher.ConnectCount()
			status.ReceiveReady = watcher.ReceiveReady()
			status.FramesReceived = watcher.FramesReceived()
			if status.WatcherConnected {
				if connectedAt := watcher.ConnectedAt(); !connectedAt.IsZero() {
					status.WatcherConnectedSeconds = int64(now.Sub(connectedAt).Seconds())
				}
			}
			if idle, known := watcher.IdleSupported(); known {
				status.IdleSupported = &idle
			}
			if last := watcher.LastFrameAt(); !last.IsZero() {
				status.LastFrameAt = last.Format(time.RFC3339Nano)
				status.LastFrameAgeMs = now.Sub(last).Milliseconds()
			}
		}
		out = append(out, status)
	}
	return out
}
