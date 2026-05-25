// Package config loads the YAML configuration file.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Mode identifies the role of this instance.
type Mode string

const (
	// ModeClient: listen on a local TCP port, relay accepted connections
	// to the peer.
	ModeClient Mode = "client"
	// ModeServer: receive OPEN frames from the peer, dial the configured
	// target, relay traffic back.
	ModeServer Mode = "server"
)

// MultipathMode controls how DATA frames are distributed across accounts.
type MultipathMode string

const (
	// MultipathModeStreamAffinity keeps every stream on one preferred
	// account. This is the default and safest mode.
	MultipathModeStreamAffinity MultipathMode = "stream_affinity"
	// MultipathModeFrameRoundRobin spreads DATA frames across proven
	// accounts, allowing one active stream to use multiple paths.
	MultipathModeFrameRoundRobin MultipathMode = "frame_round_robin"
)

// StartupCleanupConnection controls which IMAP connection removes stale
// pre-startup messages.
type StartupCleanupConnection string

const (
	// StartupCleanupConnectionDedicated uses a short-lived extra IMAP
	// connection so stale-folder cleanup cannot delay the hot receive loop.
	StartupCleanupConnectionDedicated StartupCleanupConnection = "dedicated"
	// StartupCleanupConnectionMain runs cleanup on the watcher connection.
	// This is useful for IMAP servers/accounts that reject concurrent sessions.
	StartupCleanupConnectionMain StartupCleanupConnection = "main"
	// StartupCleanupConnectionFallback tries the dedicated connection first,
	// then retries cleanup on the watcher connection if the extra connection
	// fails.
	StartupCleanupConnectionFallback StartupCleanupConnection = "fallback"
)

// MessageSubjectMode controls how Subject headers are generated.
type MessageSubjectMode string

const (
	// MessageSubjectModeFixed uses the configured message_subject.
	MessageSubjectModeFixed MessageSubjectMode = "fixed"
	// MessageSubjectModeRandom generates a fresh random subject per message.
	MessageSubjectModeRandom MessageSubjectMode = "random"
)

const DefaultMessageSubject = "TIT"

// AccountConfig is a single IMAP account participating in the tunnel.
//
// Each account contributes one independent transport path. Configuring
// multiple accounts enables multipath: outbound frames are round-robined
// across senders, and all watchers feed a shared inbound stream.
type AccountConfig struct {
	// Name is an optional human-readable label used in logs. Defaults to
	// the host if empty.
	Name string `yaml:"name"`

	// Host is "imap.example.com:993" (implicit TLS) or
	// "imap.example.com:143" (plaintext / STARTTLS).
	Host string `yaml:"host"`

	// Username for LOGIN or XOAUTH2.
	Username string `yaml:"username"`

	// Password for the LOGIN command. Set this OR one of the OAuth2
	// fields below.
	Password string `yaml:"password"`

	// OAuth2Token is a static OAuth2 access token. When set, the
	// account authenticates with SASL XOAUTH2 (required by Outlook.com
	// / Office 365 and Gmail) instead of LOGIN.
	//
	// Access tokens are short-lived (typically 1 h). Use OAuth2TokenCommand
	// in production so a fresh token is fetched on every reconnect.
	OAuth2Token string `yaml:"oauth2_token"`

	// OAuth2TokenCommand is a shell command that prints a current
	// access token on stdout. It is invoked on every (re)connect,
	// allowing an external refresher (mailctl, pizauth, oauth2l, …)
	// to manage token expiry without coordinating with this process.
	OAuth2TokenCommand string `yaml:"oauth2_token_command"`

	// TLS selects the connection security mode:
	//   "implicit" (default): connect with TLS immediately (port 993).
	//   "starttls":            connect plaintext then issue STARTTLS.
	//   "none":                connect plaintext (debug only — do NOT
	//                         use over the public internet).
	TLS string `yaml:"tls"`

	// InsecureSkipVerify disables TLS certificate verification.
	// Useful only for testing against self-signed servers.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify"`

	// FolderSend is the IMAP folder this side APPENDs into. The peer's
	// FolderRecv must point at the same folder.
	FolderSend string `yaml:"folder_send"`

	// FolderRecv is the IMAP folder this side watches via IDLE and from
	// which it deletes processed messages. The peer's FolderSend must
	// point at the same folder.
	FolderRecv string `yaml:"folder_recv"`
}

// UseOAuth2 reports whether the account is configured with OAuth2
// credentials (static token or refresh command).
func (a *AccountConfig) UseOAuth2() bool {
	return a.OAuth2Token != "" || a.OAuth2TokenCommand != ""
}

// Config is the top-level configuration.
type Config struct {
	// Mode: "client" or "server".
	Mode Mode `yaml:"mode"`

	// LogLevel is one of "error", "warn", "info" (default), "debug",
	// "trace". Overridden by the -log-level CLI flag and / or stacked
	// -v flags.
	LogLevel string `yaml:"log_level"`

	// Listen is the local TCP listen address (client mode only).
	// Example: "127.0.0.1:1080".
	Listen string `yaml:"listen"`

	// Target is the address to dial for each new stream (server mode only).
	// Example: "127.0.0.1:22".
	Target string `yaml:"target"`

	// ClientID is a 1..255 identifier required only in multi-client
	// deployments (more than one client sharing the same server / IMAP
	// folder pair). The top byte of every stream ID is stamped with this
	// value, and the client's IMAP watcher only processes / deletes
	// messages whose top byte matches. Leave 0 for single-client mode
	// (the historical behaviour). Ignored in server mode.
	ClientID uint8 `yaml:"client_id"`

	// ClientVersion is included in client Ping payloads so the server can
	// log which client build is connected. Normally filled by the CLI from
	// build metadata; YAML can override it for custom wrappers.
	ClientVersion string `yaml:"client_version"`

	// StatusAddr enables the local diagnostics HTTP API when non-empty.
	// Use "off" to disable it in environments that set a default.
	StatusAddr string `yaml:"status_addr"`

	// Accounts: at least one required.
	Accounts []AccountConfig `yaml:"accounts"`

	// Reorder controls whether the receiver buffers out-of-order frames
	// by SeqID. Recommended whenever more than one account is configured.
	// Defaults to true.
	Reorder *bool `yaml:"reorder"`

	// MultipathMode controls account selection for DATA frames:
	//
	//   "stream_affinity" (default): each stream sticks to the account
	//   selected for OPEN. Safest and lowest-jitter mode.
	//
	//   "frame_round_robin": DATA frames may be spread across connected
	//   and proven accounts. This can improve bandwidth for a single
	//   active TCP stream, but increases reordering/jitter and should be
	//   treated as experimental. OPEN/FIN/RST/control frames remain
	//   route-affine.
	MultipathMode MultipathMode `yaml:"multipath_mode"`

	// OpenTimeout bounds how long the client waits for OPEN_OK after
	// sending OPEN (default 30s).
	OpenTimeoutSec int `yaml:"open_timeout_sec"`

	// DialTimeout bounds how long the server takes to dial the target
	// before returning OPEN_FAIL (default 10s).
	DialTimeoutSec int `yaml:"dial_timeout_sec"`

	// ReconnectInitialDelayMs and ReconnectMaxDelayMs control the
	// exponential backoff used when an IMAP connection is lost.
	ReconnectInitialDelayMs int     `yaml:"reconnect_initial_delay_ms"`
	ReconnectMaxDelayMs     int     `yaml:"reconnect_max_delay_ms"`
	ReconnectBackoff        float64 `yaml:"reconnect_backoff"`

	// PollIntervalMs is the polling interval (in milliseconds) used when
	// the IMAP server does not advertise the IDLE capability, or when
	// DisableIdle is set. It is also the maximum IDLE wait before a safety
	// FETCH, so missed EXISTS notifications cannot stall the tunnel
	// indefinitely. Default 3000 (3s).
	//
	// Treated as the *idle* poll interval — used when there's no recent
	// traffic. While traffic is flowing or expected, the watcher polls
	// at ActivePollIntervalMs (see below) for low end-to-end latency.
	PollIntervalMs int `yaml:"poll_interval_ms"`

	// DisableIdle forces NOOP+FETCH polling even when the IMAP server
	// advertises IDLE/IMAP4rev2. Useful for providers with broken or
	// unreliable IDLE support, and for comparing polling behavior.
	DisableIdle bool `yaml:"disable_idle"`

	// ActivePollIntervalMs is the polling interval used during active
	// conversations: after a watcher returned new frames, or after this
	// side appended a frame (since a response is likely soon). In IDLE
	// mode it caps the active IDLE wait before a safety FETCH. Default
	// 100ms.
	ActivePollIntervalMs int `yaml:"active_poll_interval_ms"`

	// ActivePollDurationMs is how long after the last activity the
	// watcher stays in active-poll mode before reverting to the idle
	// interval. Default 5000ms.
	ActivePollDurationMs int `yaml:"active_poll_duration_ms"`

	// LazyExpungeThreshold is the number of \Deleted-marked UIDs that
	// must accumulate before the watcher EXPUNGEs them. EXPUNGE costs
	// one IMAP round-trip; batching it avoids paying that RTT on every
	// single frame and is the single biggest interactive-latency win.
	//
	// Default 16. Set to 1 to restore eager per-cycle expunge.
	LazyExpungeThreshold_ int `yaml:"lazy_expunge_threshold"`

	// LazyExpungeMaxAgeMs caps how long a \Deleted UID may sit pending
	// before being flushed, even if the threshold is not reached.
	// Prevents tail latency from leaving stale rows in the mailbox
	// during light-traffic periods. Default 30000 (30s).
	LazyExpungeMaxAgeMs int `yaml:"lazy_expunge_max_age_ms"`

	// StartupCleanupConnection controls how old messages that existed in
	// FolderRecv before watcher startup are removed:
	//   "dedicated": cleanup uses a short-lived extra connection,
	//     so startup traffic is not blocked by a large stale folder.
	//   "main": cleanup uses the watcher connection, for IMAP servers that
	//     reject multiple simultaneous connections for the same account.
	//   "fallback" (default): try "dedicated" first and retry on "main" if
	//     that fails.
	StartupCleanupConnection StartupCleanupConnection `yaml:"startup_cleanup_connection"`

	// PingIntervalMs controls the end-to-end RTT probe in client mode.
	//
	//   < 0          — disabled, no probe is ever emitted.
	//    0 (default) — fire exactly once, right after the IMAP transport
	//                  is up. Useful as a connectivity sanity-check at
	//                  startup with no ongoing overhead.
	//   > 0          — fire once at startup, then on this period in
	//                  milliseconds. Use 5000–30000 for diagnostic runs.
	//
	// Each probe sends a Ping carrying its send timestamp; the peer
	// echoes it back as Pong, and the client logs the round-trip at
	// INFO level. Ignored in server mode (the server only echoes).
	PingIntervalMs int `yaml:"ping_interval_ms"`

	// MessageFormat selects how the IMAP message body is structured.
	//
	//   "attachment" (default): the body is presented as a binary
	//     attachment (Content-Type: application/octet-stream, CTE
	//     base64, with a Content-Disposition: attachment filename).
	//     This is the historical behavior. Some mail clients render
	//     such messages with an obvious paperclip icon and an "Open
	//     attachment" UI.
	//
	//   "text": the body is presented as plain text containing the
	//     base64 of the frame (Content-Type: text/plain, CTE 7bit,
	//     base64 wrapped to 76 chars per line). Mail clients render
	//     this as an ordinary email with a base64 blob in the body —
	//     no attachment indicator. Slightly less robust if the IMAP
	//     server prepends/appends text (e.g. "external sender" banners
	//     or signatures); attachment mode resists that better.
	MessageFormat string `yaml:"message_format"`

	// AttachmentFilename is the filename hint embedded in the
	// Content-Type / Content-Disposition headers when MessageFormat is
	// "attachment". Default "tunnel.bin". Some mail clients show this
	// in the attachment UI instead of "Untitled.bin".
	AttachmentFilename string `yaml:"attachment_filename"`

	// MessageSubject is the Subject header used when MessageSubjectMode is
	// "fixed". Default "TIT", preserving the historical hardcoded marker.
	MessageSubject string `yaml:"message_subject"`

	// MessageSubjectMode selects fixed or per-message random Subject headers.
	// The receiver ignores Subject, so endpoints do not have to match.
	MessageSubjectMode MessageSubjectMode `yaml:"message_subject_mode"`

	// SubjectClientID prefixes each Subject with the frame's client ID so
	// multi-client receivers can reject other clients' messages after a
	// lightweight header fetch. Default true; set false to avoid leaking
	// client IDs in mail headers.
	SubjectClientID *bool `yaml:"subject_client_id"`

	// EncryptionPassphrase enables AES-256-GCM encryption of the frame
	// bytes carried inside each IMAP message. The passphrase is hashed
	// (SHA-256) to derive the 32-byte key; both sides of the tunnel
	// MUST use the same value or every received frame will fail to
	// decrypt and be dropped.
	//
	// The IMAP transport itself is already protected by TLS — this
	// layer only adds *content concealment* against the email provider
	// (which can read stored messages in cleartext). Each frame uses
	// a fresh random nonce, so identical plaintexts produce different
	// ciphertexts.
	//
	// Empty disables encryption.
	EncryptionPassphrase string `yaml:"encryption_passphrase"`

	// ClientEncryptionPassphrases maps stream client IDs to dedicated
	// passphrases. It is intended for server mode in multi-client
	// deployments: each client keeps using encryption_passphrase with only
	// its own secret, while the server selects the matching key by client ID.
	// A global EncryptionPassphrase may still be set as a legacy fallback.
	ClientEncryptionPassphrases map[byte]string `yaml:"client_encryption_passphrases"`

	// DNSServers, when non-empty, replaces Go's default resolver with
	// a custom one that dials these servers directly instead of
	// reading /etc/resolv.conf. Each entry is "host:port" (e.g.
	// "1.1.1.1:53"). Bare addresses get ":53" appended.
	//
	// When empty, the CLI uses the local/system resolver first and may
	// install a default 1.1.1.1 fallback if the local resolver fails at
	// startup.
	DNSServers []string `yaml:"dns_servers"`

	// PidFile, when set, activates stale-process cleanup on startup.
	// On start the binary reads the PID from this file (if it exists),
	// checks whether that process is still alive, and kills it before
	// binding the listen port. On exit it removes the file.
	//
	// This prevents "address already in use" errors when the parent
	// process is killed without a chance to stop the tunnel, leaving
	// the old tunnel binary running.
	PidFile string `yaml:"pid_file"`

	// GracefulShutdownMs bounds how long the tunnel spends, on exit,
	// emitting RST frames for still-open streams so the peer can
	// release its bookkeeping cleanly. Default 3000ms. Set to 0 to
	// skip the courtesy notification entirely (peer will time out the
	// orphans on its own).
	GracefulShutdownMs int `yaml:"graceful_shutdown_ms"`

	// BatchDelayMs is a short pause (in milliseconds) after the first
	// frame arrives in an idle sender before draining the queue and
	// APPENDing. This gives frames from other streams a chance to
	// arrive and be batched into the same APPEND — saving an entire
	// IMAP round-trip at the cost of a tiny added latency.
	//
	// Only matters at the start of a burst (sender was idle). When
	// traffic is already flowing, frames pile up during the in-flight
	// APPEND and are drained opportunistically — the delay is not
	// applied in that case.
	//
	// Default 2ms. Set explicitly to 0 to disable for minimum
	// per-connection latency.
	BatchDelayMs int `yaml:"batch_delay_ms"`

	// BatchDelayMsSet tracks whether batch_delay_ms was present in YAML
	// or SIP003 options, so an explicit 0 can disable the 2ms default.
	BatchDelayMsSet bool `yaml:"-"`

	// BatchMaxFrames caps the number of frames packed into a single
	// IMAP APPEND. When many TCP streams write concurrently they share
	// one sender; the sender's drain loop opportunistically packs
	// whatever piled up during the previous APPEND into one batch,
	// up to this many frames. Default 64. Setting to 1 disables
	// cross-stream batching (every frame becomes its own APPEND).
	BatchMaxFrames_ int `yaml:"batch_max_frames"`

	// BatchMaxKB is a parallel cap on the encoded batch size, in KiB.
	// Default 256. The smaller of (BatchMaxFrames, BatchMaxKB) wins.
	BatchMaxKB int `yaml:"batch_max_kb"`

	// BatchQueueSize_ is the per-sender outbound queue depth. Frames
	// arriving while the queue is full block their callers (back-
	// pressure). Default 256.
	BatchQueueSize_ int `yaml:"batch_queue_size"`

	// AsyncDataSend, when true, makes DATA frames return after they are
	// enqueued to the IMAP sender instead of waiting for the APPEND to
	// complete. Control frames (OPEN/FIN/RST/PING/etc.) still wait for
	// completion so lifecycle errors surface promptly.
	//
	// This can improve throughput by letting TCP read loops continue
	// filling the sender queue while an APPEND is in flight. The sender
	// queue remains bounded, so back-pressure still applies when IMAP
	// cannot keep up. Default false (conservative error propagation).
	AsyncDataSend bool `yaml:"async_data_send"`

	// InboundQueueSize_ is the per-stream inbound (TCP-write) buffer
	// depth on the receive side. Frames arriving from the IMAP watcher
	// are enqueued here; a dedicated per-stream writer goroutine drains
	// them onto the TCP socket. When the queue fills, a per-stream
	// overflow drainer waits up to inbound_queue_wait_ms for the TCP
	// consumer to catch up before resetting the stream. Default 64.
	InboundQueueSize_ int `yaml:"inbound_queue_size"`

	// InboundQueueWaitMs is the maximum time overflowed incoming DATA/FIN
	// may wait for per-stream TCP-write queue space before treating the
	// target TCP side as stuck and resetting the stream. This applies
	// normal backpressure to uploads without blocking the shared watcher
	// dispatch loop. Default 30000ms. Set <0 for immediate-reset behavior.
	InboundQueueWaitMs int `yaml:"inbound_queue_wait_ms"`

	// ZeroRTTOpen, when true, lets the client start sending DATA
	// immediately after OPEN without waiting for OPEN_OK. Saves one
	// end-to-end round-trip per new TCP stream — visible as a faster
	// first byte for client-first protocols (SSH, HTTP).
	//
	// **Default is false** because real-world experience with SOCKS5 /
	// HTTPS workloads has shown that on cold connections (the first
	// time a stream is opened through a fresh IMAP session), the
	// early-DATA path can race in ways that cause the first browser
	// request to fail. A retry typically succeeds. Until we have a
	// reproducible test for the failure, the safer default is the
	// classic OPEN → OPEN_OK → DATA handshake.
	//
	// The server-side state machine always handles 0-RTT correctly
	// (DATA arriving before its dial completes is buffered and flushed
	// once the target is connected); this flag only changes the
	// CLIENT'S handshake — turning it on is safe to do unilaterally.
	//
	// On dial failure, the client receives OPEN_FAIL and tears the TCP
	// socket down; any DATA it had already sent is discarded by the
	// server. For protocols with large client-first payloads this is a
	// small bandwidth cost; for interactive workloads it's negligible.
	ZeroRTTOpen *bool `yaml:"zero_rtt_open"`
}

// UnmarshalYAML records presence for fields where zero is an explicit,
// non-default value.
func (c *Config) UnmarshalYAML(value *yaml.Node) error {
	type configAlias Config
	var raw configAlias
	if err := value.Decode(&raw); err != nil {
		return err
	}
	*c = Config(raw)
	for i := 0; i+1 < len(value.Content); i += 2 {
		if value.Content[i].Value == "batch_delay_ms" {
			c.BatchDelayMsSet = true
			break
		}
	}
	return nil
}

// ReorderEnabled returns the effective reorder setting. Defaults to true
// when unset (the safe choice with multipath).
func (c *Config) ReorderEnabled() bool {
	if c.Reorder == nil {
		return true
	}
	return *c.Reorder
}

// EffectiveMultipathMode returns the DATA-frame routing mode.
func (c *Config) EffectiveMultipathMode() MultipathMode {
	switch MultipathMode(strings.ToLower(strings.TrimSpace(string(c.MultipathMode)))) {
	case "", MultipathModeStreamAffinity:
		return MultipathModeStreamAffinity
	case MultipathModeFrameRoundRobin:
		return MultipathModeFrameRoundRobin
	default:
		return c.MultipathMode
	}
}

// FrameRoundRobinEnabled reports whether DATA frames may be distributed
// independently of their stream's preferred account.
func (c *Config) FrameRoundRobinEnabled() bool {
	return c.EffectiveMultipathMode() == MultipathModeFrameRoundRobin
}

// EffectiveStartupCleanupConnection returns the startup stale-message cleanup
// connection mode. The default keeps the low-latency dedicated path and retries
// on the watcher connection when a server rejects the extra connection.
func (c *Config) EffectiveStartupCleanupConnection() StartupCleanupConnection {
	switch StartupCleanupConnection(strings.ToLower(strings.TrimSpace(string(c.StartupCleanupConnection)))) {
	case "":
		return StartupCleanupConnectionFallback
	case StartupCleanupConnectionDedicated:
		return StartupCleanupConnectionDedicated
	case StartupCleanupConnectionMain:
		return StartupCleanupConnectionMain
	case StartupCleanupConnectionFallback:
		return StartupCleanupConnectionFallback
	default:
		return c.StartupCleanupConnection
	}
}

// OpenTimeout returns the OPEN response timeout.
func (c *Config) OpenTimeout() time.Duration {
	if c.OpenTimeoutSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.OpenTimeoutSec) * time.Second
}

// DialTimeout returns the target-dial timeout.
func (c *Config) DialTimeout() time.Duration {
	if c.DialTimeoutSec <= 0 {
		return 10 * time.Second
	}
	return time.Duration(c.DialTimeoutSec) * time.Second
}

// ReconnectInitialDelay returns the initial reconnect delay.
func (c *Config) ReconnectInitialDelay() time.Duration {
	if c.ReconnectInitialDelayMs <= 0 {
		return 500 * time.Millisecond
	}
	return time.Duration(c.ReconnectInitialDelayMs) * time.Millisecond
}

// ReconnectMaxDelay returns the maximum reconnect delay.
func (c *Config) ReconnectMaxDelay() time.Duration {
	if c.ReconnectMaxDelayMs <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.ReconnectMaxDelayMs) * time.Millisecond
}

// ReconnectBackoffMultiplier returns the backoff factor (>1.0).
func (c *Config) ReconnectBackoffMultiplier() float64 {
	if c.ReconnectBackoff <= 1.0 {
		return 1.5
	}
	return c.ReconnectBackoff
}

// PollInterval returns the NOOP poll interval used when IDLE is not
// supported (idle / no-traffic case).
func (c *Config) PollInterval() time.Duration {
	if c.PollIntervalMs <= 0 {
		return 3 * time.Second
	}
	return time.Duration(c.PollIntervalMs) * time.Millisecond
}

// ActivePollInterval returns the short poll interval used during active
// conversations.
func (c *Config) ActivePollInterval() time.Duration {
	if c.ActivePollIntervalMs <= 0 {
		return 100 * time.Millisecond
	}
	return time.Duration(c.ActivePollIntervalMs) * time.Millisecond
}

// ActivePollDuration returns how long active-poll mode lasts after the
// last activity.
func (c *Config) ActivePollDuration() time.Duration {
	if c.ActivePollDurationMs <= 0 {
		return 5 * time.Second
	}
	return time.Duration(c.ActivePollDurationMs) * time.Millisecond
}

// LazyExpungeThreshold returns the pending-deletion count above which
// EXPUNGE is flushed.
func (c *Config) LazyExpungeThreshold() int {
	if c.LazyExpungeThreshold_ <= 0 {
		return 16
	}
	return c.LazyExpungeThreshold_
}

// LazyExpungeMaxAge returns the pending-deletion staleness limit above
// which EXPUNGE is flushed even if the threshold is not reached.
func (c *Config) LazyExpungeMaxAge() time.Duration {
	if c.LazyExpungeMaxAgeMs <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.LazyExpungeMaxAgeMs) * time.Millisecond
}

// PingEnabled reports whether the RTT probe should run at all
// (negative PingIntervalMs disables it entirely).
func (c *Config) PingEnabled() bool {
	return c.PingIntervalMs >= 0
}

// PingPeriodic reports whether the probe should repeat after the
// initial fire (true) or run exactly once and stop (false).
func (c *Config) PingPeriodic() bool {
	return c.PingIntervalMs > 0
}

// PingInterval returns the repeat period when PingPeriodic() is true.
// Otherwise it returns 0.
func (c *Config) PingInterval() time.Duration {
	if c.PingIntervalMs <= 0 {
		return 0
	}
	return time.Duration(c.PingIntervalMs) * time.Millisecond
}

// EffectiveMessageFormat returns the message-body format, normalized to
// lowercase and defaulted to "attachment" when unset.
func (c *Config) EffectiveMessageFormat() string {
	switch f := strings.ToLower(strings.TrimSpace(c.MessageFormat)); f {
	case "", "attachment":
		return "attachment"
	case "text":
		return "text"
	default:
		return f
	}
}

// EffectiveAttachmentFilename returns the attachment filename hint,
// defaulting to "tunnel.bin" when unset.
func (c *Config) EffectiveAttachmentFilename() string {
	if s := strings.TrimSpace(c.AttachmentFilename); s != "" {
		return s
	}
	return "tunnel.bin"
}

// EffectiveMessageSubject returns the fixed Subject header value, defaulting
// to the historical "TIT" marker when unset.
func (c *Config) EffectiveMessageSubject() string {
	if s := strings.TrimSpace(c.MessageSubject); s != "" {
		return s
	}
	return DefaultMessageSubject
}

// EffectiveMessageSubjectMode returns the Subject generation mode, normalized
// to lowercase and defaulted to "fixed".
func (c *Config) EffectiveMessageSubjectMode() MessageSubjectMode {
	switch mode := MessageSubjectMode(strings.ToLower(strings.TrimSpace(string(c.MessageSubjectMode)))); mode {
	case "", MessageSubjectModeFixed:
		return MessageSubjectModeFixed
	case MessageSubjectModeRandom:
		return MessageSubjectModeRandom
	default:
		return mode
	}
}

// SubjectClientIDEnabled reports whether messages should include a parseable
// client-ID tag in the Subject header. It defaults on for compatibility with
// newer multi-client receivers while remaining opt-out.
func (c *Config) SubjectClientIDEnabled() bool {
	if c.SubjectClientID == nil {
		return true
	}
	return *c.SubjectClientID
}

// GracefulShutdown returns how long the tunnel may spend emitting RSTs
// on exit. Zero / negative means skip the courtesy notification.
func (c *Config) GracefulShutdown() time.Duration {
	if c.GracefulShutdownMs < 0 {
		return 0
	}
	if c.GracefulShutdownMs == 0 {
		return 3 * time.Second
	}
	return time.Duration(c.GracefulShutdownMs) * time.Millisecond
}

// BatchMaxFrames returns the max number of frames per IMAP APPEND.
func (c *Config) BatchMaxFrames() int {
	if c.BatchMaxFrames_ <= 0 {
		return 64
	}
	return c.BatchMaxFrames_
}

// BatchMaxBytes returns the max encoded-batch size in bytes.
func (c *Config) BatchMaxBytes() int {
	if c.BatchMaxKB <= 0 {
		return 256 * 1024
	}
	return c.BatchMaxKB * 1024
}

// BatchQueueSize returns the per-sender outbound queue depth.
func (c *Config) BatchQueueSize() int {
	if c.BatchQueueSize_ <= 0 {
		return 256
	}
	return c.BatchQueueSize_
}

// BatchDelay returns the batch coalescing delay. Default 2ms.
func (c *Config) BatchDelay() time.Duration {
	if c.BatchDelayMsSet && c.BatchDelayMs <= 0 {
		return 0
	}
	if c.BatchDelayMs <= 0 {
		return 2 * time.Millisecond
	}
	return time.Duration(c.BatchDelayMs) * time.Millisecond
}

// AsyncDataSendEnabled reports whether DATA frames should return after
// sender-queue enqueue instead of waiting for APPEND completion.
func (c *Config) AsyncDataSendEnabled() bool { return c.AsyncDataSend }

// InboundQueueSize returns the per-stream inbound (TCP-write) queue depth.
func (c *Config) InboundQueueSize() int {
	if c.InboundQueueSize_ <= 0 {
		return 64
	}
	return c.InboundQueueSize_
}

// InboundQueueWait returns the per-stream TCP-write backpressure window.
func (c *Config) InboundQueueWait() time.Duration {
	if c.InboundQueueWaitMs < 0 {
		return 0
	}
	if c.InboundQueueWaitMs == 0 {
		return 30 * time.Second
	}
	return time.Duration(c.InboundQueueWaitMs) * time.Millisecond
}

// ZeroRTTOpenEnabled returns the effective zero-RTT setting. Defaults
// to false — historical experience with real-world SOCKS5/HTTPS
// workloads showed that the 1-RTT it saves is not worth the
// reduced compatibility (early DATA can race against server-side
// dial completion under certain conditions). Users who want the
// latency win and have verified their workload tolerates it can
// opt in explicitly.
func (c *Config) ZeroRTTOpenEnabled() bool {
	if c.ZeroRTTOpen == nil {
		return false
	}
	return *c.ZeroRTTOpen
}

// Load reads and validates a configuration file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return parseAndValidate(data)
}

// LoadReader reads a YAML configuration from an arbitrary io.Reader.
func LoadReader(r io.Reader) (*Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return parseAndValidate(data)
}

// LoadBytes parses a YAML configuration from bytes.
func LoadBytes(data []byte) (*Config, error) { return parseAndValidate(data) }

func parseAndValidate(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate checks that required fields are set and consistent.
func (c *Config) Validate() error {
	switch c.Mode {
	case ModeClient:
		if c.Listen == "" {
			return errors.New("client mode requires `listen`")
		}
	case ModeServer:
		if c.Target == "" {
			return errors.New("server mode requires `target`")
		}
	default:
		return fmt.Errorf("invalid mode %q (expected %q or %q)", c.Mode, ModeClient, ModeServer)
	}
	if len(c.Accounts) == 0 {
		return errors.New("at least one IMAP account is required")
	}
	for i := range c.Accounts {
		a := &c.Accounts[i]
		if a.Host == "" {
			return fmt.Errorf("account #%d: missing host", i+1)
		}
		if a.Username == "" {
			return fmt.Errorf("account %q: missing username", a.label())
		}
		hasPwd := a.Password != ""
		hasOAuth := a.UseOAuth2()
		if !hasPwd && !hasOAuth {
			return fmt.Errorf("account %q: missing credentials (set password OR oauth2_token / oauth2_token_command)", a.label())
		}
		if hasPwd && hasOAuth {
			return fmt.Errorf("account %q: both password and oauth2 credentials set — pick one", a.label())
		}
		if a.OAuth2Token != "" && a.OAuth2TokenCommand != "" {
			return fmt.Errorf("account %q: both oauth2_token and oauth2_token_command set — pick one", a.label())
		}
		if a.FolderSend == "" {
			return fmt.Errorf("account %q: missing folder_send", a.label())
		}
		if a.FolderRecv == "" {
			return fmt.Errorf("account %q: missing folder_recv", a.label())
		}
		if a.TLS == "" {
			a.TLS = "implicit"
		}
		switch a.TLS {
		case "implicit", "starttls", "none":
		default:
			return fmt.Errorf("account %q: invalid tls %q (expected implicit, starttls or none)", a.label(), a.TLS)
		}
	}
	switch c.EffectiveMessageFormat() {
	case "attachment", "text":
	default:
		return fmt.Errorf("invalid message_format %q (expected \"attachment\" or \"text\")", c.MessageFormat)
	}
	switch c.EffectiveMessageSubjectMode() {
	case MessageSubjectModeFixed, MessageSubjectModeRandom:
	default:
		return fmt.Errorf("invalid message_subject_mode %q (expected \"fixed\" or \"random\")", c.MessageSubjectMode)
	}
	if strings.ContainsAny(c.MessageSubject, "\r\n") {
		return fmt.Errorf("invalid message_subject %q (must not contain CR or LF)", c.MessageSubject)
	}
	for id, passphrase := range c.ClientEncryptionPassphrases {
		if id == 0 {
			return fmt.Errorf("invalid client_encryption_passphrases key 0 (expected 1-255)")
		}
		if strings.TrimSpace(passphrase) == "" {
			return fmt.Errorf("invalid client_encryption_passphrases[%d]: passphrase must not be empty", id)
		}
	}
	switch c.EffectiveMultipathMode() {
	case MultipathModeStreamAffinity, MultipathModeFrameRoundRobin:
	default:
		return fmt.Errorf("invalid multipath_mode %q (expected \"stream_affinity\" or \"frame_round_robin\")", c.MultipathMode)
	}
	switch c.EffectiveStartupCleanupConnection() {
	case StartupCleanupConnectionDedicated, StartupCleanupConnectionMain, StartupCleanupConnectionFallback:
	default:
		return fmt.Errorf("invalid startup_cleanup_connection %q (expected \"dedicated\", \"main\" or \"fallback\")", c.StartupCleanupConnection)
	}
	return nil
}

// Label returns a short identifier for logs.
func (a *AccountConfig) Label() string { return a.label() }

func (a *AccountConfig) label() string {
	if a.Name != "" {
		return a.Name
	}
	return a.Host
}
